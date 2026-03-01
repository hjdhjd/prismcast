/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * table.ts: Channel table rendering for the PrismCast configuration interface.
 */
import { compareChannelSort, getAllProviderTags, getChannelProviderLabel, getChannelProviderTags, getChannelSortKey, getEnabledProviders,
  getProviderGroup, getProviderSelection, getProviderTagForChannel, getResolvedChannel, hasMultipleProviders, isChannelAvailableByProvider,
  isProviderTagEnabled, resolveProviderKey } from "../../../config/providers.js";
import { escapeHtml, formatTimeAgo } from "../../../utils/index.js";
import { getCachedProviderChannels, getProviderDomainMap, getProviderGuideUrls } from "../../../browser/channelSelection.js";
import { getChannelHealth, getProviderAuth } from "../../../config/health.js";
import { getChannelListing, getChannelsParseErrorMessage, getPredefinedScopeCounts, getUserChannelsFilePath, hasChannelsParseError, isPredefinedChannel,
  isPredefinedChannelDisabled, isUserChannel } from "../../../config/userChannels.js";
import { getProfileForChannel, getProfiles } from "../../../config/profiles.js";
import { CONFIG } from "../../../config/index.js";
import type { ChannelListingEntry } from "../../../types/index.js";
import { PREDEFINED_CHANNELS } from "../../../channels/index.js";
import type { ProfileInfo } from "../../../config/profiles.js";
import { categorizeProfiles } from "../index.js";

// SVG icon constants for channel action buttons. Each icon is 14x14px with a 16x16 viewBox, stroke-based with currentColor, and uses round line caps/joins.

export const ICON_EDIT = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><path d=\"M11.5 1.5l3 3L5 14H2v-3L11.5 1.5z\"/></svg>";

const ICON_LOGIN = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><path d=\"M6.5 2H3.5a1 1 0 00-1 1v10a1 1 0 001 1h3\"/><path d=\"M10.5 11l3-3-3-3\"/><path d=\"M13.5 8H6.5\"/></svg>";

export const ICON_DELETE = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><path d=\"M2 4h12\"/><path d=\"M5 4V2.5a.5.5 0 01.5-.5h5a.5.5 0 01.5.5V4\"/><path d=\"M12.5 4l-.5 9.5a1 1 0 01-1 .5H5a1 1 0 " +
  "01-1-.5L3.5 4\"/></svg>";

const ICON_ENABLE = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><circle cx=\"8\" cy=\"8\" r=\"6\"/><path d=\"M5.5 8l2 2 3.5-4\"/></svg>";

const ICON_DISABLE = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><circle cx=\"8\" cy=\"8\" r=\"6\"/><path d=\"M5.5 5.5l5 5\"/></svg>";

const ICON_HEALTH = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" " +
  "stroke-linecap=\"round\" stroke-linejoin=\"round\"><polyline points=\"1,9 4,9 6,4 8,12 10,7 12,9 15,9\"/></svg>";

const ICON_REVERT = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><path d=\"M3 8a5 5 0 1 1 1.5 3.5\"/><path d=\"M3 4v4h4\"/></svg>";

const ICON_COPY = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" " +
  "stroke-linejoin=\"round\"><rect x=\"5\" y=\"5\" width=\"9\" height=\"9\" rx=\"1\"/><path d=\"M5 11H3a1 1 0 01-1-1V3a1 1 0 011-1h7a1 1 0 011 1v2\"/></svg>";

// Optional column definitions for the channels table. These columns can be shown or hidden by the user via the column picker. The order here determines the
// column order in the table, slotted between Provider and Actions.
export const OPTIONAL_COLUMNS: readonly {
  readonly align: string; readonly cssClass: string; readonly field: string; readonly label: string; readonly width: string;
}[] = [

  { align: "center", cssClass: "col-chnum", field: "channelNumber", label: "Number", width: "70px" },
  { align: "center", cssClass: "col-stationid", field: "stationId", label: "Station ID", width: "100px" },
  { align: "left", cssClass: "col-profile", field: "profile", label: "Profile", width: "130px" },
  { align: "left", cssClass: "col-selector", field: "channelSelector", label: "Selector", width: "130px" }
];

// Total number of columns in the channels table (4 required + 4 optional).
const TOTAL_COLUMN_COUNT = 8;

// Valid optional column field names.
export const VALID_OPTIONAL_COLUMNS = new Set(OPTIONAL_COLUMNS.map((c) => c.field));

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

  // Multi-channel profiles.
  if(groups.multiChannel.length > 0) {

    lines.push("<optgroup label=\"Multi-Channel (needs selector)\">");

    for(const profile of groups.multiChannel) {

      lines.push(renderOption(profile));
    }

    lines.push("</optgroup>");
  }

  // Custom (user-defined) profiles.
  if(groups.custom.length > 0) {

    lines.push("<optgroup label=\"Custom\">");

    for(const profile of groups.custom) {

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

  // Custom (user-defined) profiles.
  if(groups.custom.length > 0) {

    lines.push("<div class=\"profile-category\">");
    lines.push("<h4>Custom Profiles</h4>");
    lines.push("<p class=\"category-desc\">User-defined profiles created via the profile builder wizard or imported from provider packs.</p>");
    lines.push("<dl class=\"profile-list\">");

    for(const profile of groups.custom) {

      lines.push("<dt>" + escapeHtml(profile.name) + "</dt>");
      lines.push("<dd>" + escapeHtml(profile.description || "No description provided.") + "</dd>");
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
 * Generates JavaScript variables for channel selector datalist population. Produces three variables: `channelSelectorsByDomain` maps URL hostnames to known
 * channel selector values (from predefined channels and cached provider discovery), `providerByDomain` maps provider guide URL hostnames to provider slugs for
 * client-side async discovery, and `providerGuideUrl` maps provider slugs to their guide URLs for URL correction hints. Embedded as a `<script>` block in the
 * channels panel.
 *
 * @returns JavaScript variable declarations ready to embed in a `<script>` tag.
 */
function generateChannelSelectorData(): string {

  const byDomain: Record<string, { label: string; value: string }[]> = {};
  const seen: Record<string, Set<string>> = {};

  for(const channel of Object.values(PREDEFINED_CHANNELS)) {

    if(!channel.channelSelector) {

      continue;
    }

    const hostname = new URL(channel.url).hostname;

    seen[hostname] ??= new Set();

    if(seen[hostname].has(channel.channelSelector)) {

      continue;
    }

    seen[hostname].add(channel.channelSelector);
    byDomain[hostname] ??= [];
    byDomain[hostname].push({ label: channel.name ?? channel.channelSelector, value: channel.channelSelector });
  }

  // Merge cached provider-discovered channels into the domain map. Predefined entries take precedence — we only add discovered channels whose channelSelector
  // value is not already present for that domain. This enriches the datalist with the full provider lineup when precaching or prior discovery has run.
  for(const provider of getCachedProviderChannels()) {

    seen[provider.hostname] ??= new Set();
    byDomain[provider.hostname] ??= [];

    for(const entry of provider.entries) {

      if(!seen[provider.hostname].has(entry.value)) {

        seen[provider.hostname].add(entry.value);
        byDomain[provider.hostname].push(entry);
      }
    }
  }

  // Build the hostname→slug map and slug→guideUrl map for all providers (including those with cold caches) so the client-side fetch can trigger discovery for any
  // provider domain and the URL hint can suggest the correct guide URL.
  const providerByDomain = getProviderDomainMap();
  const providerGuideUrl = getProviderGuideUrls();

  // Sort entries within each domain alphabetically by label for consistent ordering in the datalist dropdown.
  for(const entries of Object.values(byDomain)) {

    entries.sort((a, b) => a.label.localeCompare(b.label));
  }

  return "var channelSelectorsByDomain = " + JSON.stringify(byDomain) + ";\n" +
    "var providerByDomain = " + JSON.stringify(providerByDomain) + ";\n" +
    "var providerGuideUrl = " + JSON.stringify(providerGuideUrl) + ";";
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
  displayLines.push("<td class=\"ch-key\" data-sort-value=\"" + escapeHtml(getChannelSortKey(channel, key, "key")) + "\">" + escapeHtml(key) + "</td>");
  displayLines.push("<td data-sort-value=\"" + escapeHtml(getChannelSortKey(channel, key, "name")) + "\">" + escapeHtml(channel.name ?? key) + "</td>");

  // Provider column: dropdown for multi-provider channels, static provider name for single-provider. Both states always render a hidden "No available providers"
  // label alongside the provider content so that client-side filterChannelRows() can toggle between them without a page reload.
  displayLines.push("<td data-sort-value=\"" + escapeHtml(getChannelSortKey(channel, key, "provider")) + "\">");

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

    // Single-provider: wrap the provider name in a span so client-side JS can toggle it with the no-provider label. Uses profile-aware label resolution so
    // channels with explicit profile assignments show the profile's provider name rather than the built-in name for the URL domain.
    displayLines.push("<span class=\"provider-name\"" + contentHidden + ">" +
      escapeHtml(getChannelProviderLabel(channel)) + "</span>");
  }

  displayLines.push("</td>");

  // Optional columns: Number, Station ID, Profile, Selector. All four are always rendered; visibility is controlled by CSS classes on the table element.
  displayLines.push("<td class=\"col-chnum\" data-sort-value=\"" + escapeHtml(getChannelSortKey(channel, key, "channelNumber")) + "\">" +
    (channel.channelNumber ? escapeHtml(String(channel.channelNumber)) : "") + "</td>");
  displayLines.push("<td class=\"col-stationid\" data-sort-value=\"" + escapeHtml(getChannelSortKey(channel, key, "stationId")) + "\">" +
    (channel.stationId ? escapeHtml(channel.stationId) : "") + "</td>");

  // Profile column: show explicit profile as-is, or the auto-resolved friendly name with "(auto)" suffix in muted style. The sort key resolves the selected
  // provider variant internally via getChannelSortKey. The display content uses displayChannel because profile resolution is URL-dependent and a canonical's URL
  // may differ from the selected variant's.
  const profileSortKey = escapeHtml(getChannelSortKey(channel, key, "profile"));

  if(displayChannel.profile) {

    displayLines.push("<td class=\"col-profile\" data-sort-value=\"" + profileSortKey + "\">" + escapeHtml(displayChannel.profile) + "</td>");
  } else {

    const resolved = getProfileForChannel(displayChannel);

    if(resolved.profileName !== "default") {

      const label = getChannelProviderLabel(displayChannel);

      displayLines.push("<td class=\"col-profile\" data-sort-value=\"" + profileSortKey +
        "\"><span class=\"text-muted\">" + escapeHtml(label + " (auto)") + "</span></td>");
    } else {

      displayLines.push("<td class=\"col-profile\" data-sort-value=\"" + profileSortKey + "\"></td>");
    }
  }

  displayLines.push("<td class=\"col-selector\" data-sort-value=\"" + escapeHtml(getChannelSortKey(channel, key, "channelSelector")) + "\">" +
    (channel.channelSelector ? escapeHtml(channel.channelSelector) : "<span class=\"text-muted\">&ndash;</span>") + "</td>");

  // Actions column with icon buttons. Five positions per row: Edit (always), Login/placeholder, Health/placeholder, context-sensitive, and Copy URL.
  displayLines.push("<td>");
  displayLines.push("<div class=\"btn-group\">");

  const escapedKey = escapeHtml(key);

  // Resolve the provider tag for the currently selected provider variant. Used for both login icon color and health icon lookups.
  const variantKey = resolveProviderKey(key);
  const providerTag = getProviderTagForChannel(variantKey);

  // Position 1: Edit (all channels).
  displayLines.push("<button type=\"button\" class=\"btn-icon btn-icon-edit\" title=\"Edit\" aria-label=\"Edit\" onclick=\"showEditForm('" + escapedKey +
    "')\">" + ICON_EDIT + "</button>");

  // Position 2: Login for enabled channels (with provider auth color), placeholder for disabled predefined. Custom channels (user-defined, not predefined) skip
  // login coloring because they have no provider concept.
  if(!isDisabled) {

    const authTimestamp = (isPredefined || isOverride) ? getProviderAuth(providerTag) : null;
    const loginColorClass = authTimestamp ? " health-success" : "";
    const loginTitle = authTimestamp ? "Verified " + formatTimeAgo(authTimestamp) : "Not yet verified";

    displayLines.push("<button type=\"button\" class=\"btn-icon btn-icon-login" + loginColorClass + "\" data-provider-tag=\"" +
      escapeHtml(providerTag) + "\" title=\"" + loginTitle + "\" aria-label=\"Login\" " +
      "onclick=\"startChannelLogin('" + escapedKey + "')\">" + ICON_LOGIN + "</button>");
  } else {

    displayLines.push("<span class=\"btn-icon-placeholder\"></span>");
  }

  // Position 3: Channel health indicator. Shows last tune result via color. Non-interactive (span, not button). Disabled channels get a placeholder instead.
  if(!isDisabled) {

    const channelHealthResult = getChannelHealth(key, providerTag);
    const healthColorClass = (channelHealthResult?.status === "success") ? " health-success" : (channelHealthResult?.status === "failed") ? " health-failed" : "";
    const healthTitle = channelHealthResult ?
      (channelHealthResult.status === "success" ? "Succeeded " : "Failed ") + formatTimeAgo(channelHealthResult.timestamp) : "Not yet tuned";

    displayLines.push("<span class=\"btn-icon btn-icon-health" + healthColorClass + "\" title=\"" + healthTitle +
      "\" aria-label=\"Channel health\">" + ICON_HEALTH + "</span>");
  } else {

    displayLines.push("<span class=\"btn-icon-placeholder\"></span>");
  }

  // Position 4: varies by row type.
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

  // Position 5: Copy URL dropdown (all channels).
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
  editLines.push("<td colspan=\"" + String(TOTAL_COLUMN_COUNT) + "\">");
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
 * Generates the provider filter toolbar HTML with a multi-select dropdown and dismissable chips.
 * @returns HTML string for the provider filter toolbar.
 */
export function generateProviderFilterToolbar(): string {

  const allTags = getAllProviderTags();
  const enabled = getEnabledProviders();
  const hasFilter = enabled.length > 0;
  const lines: string[] = [];

  lines.push("<div class=\"provider-toolbar\">");

  // Provider filter dropdown and chips.
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

  // Toolbar with channel operations.
  lines.push("<div class=\"channel-toolbar\">");

  // Channel operations. Import uses a dropdown menu to consolidate M3U and JSON import into a single button.
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

  lines.push("<div class=\"dropdown quick-actions-dropdown\">");
  lines.push("<button type=\"button\" class=\"btn btn-secondary btn-sm\" onclick=\"toggleDropdown(this)\">Quick Actions &#9662;</button>");
  lines.push("<div class=\"dropdown-menu\">");
  // Compute initial toggle counts for predefined channel scopes. The server is the single source of truth — the client renders what we return here.
  const scopeCounts = getPredefinedScopeCounts();

  // Three toggle rows: checkbox + label + "X of Y enabled" count. Clicking toggles the group via bulkTogglePredefined(). The onclick uses event.preventDefault()
  // to stop the native checkbox toggle — the server response drives the update.
  const scopes: { count: number; label: string; scope: string; total: number }[] = [
    { count: scopeCounts.all.enabled, label: "All Predefined", scope: "all", total: scopeCounts.all.total },
    { count: scopeCounts.east.enabled, label: "East Variants", scope: "east", total: scopeCounts.east.total },
    { count: scopeCounts.pacific.enabled, label: "Pacific Variants", scope: "pacific", total: scopeCounts.pacific.total }
  ];

  for(const s of scopes) {

    const checked = (s.count === s.total) ? " checked" : "";

    lines.push("<label class=\"provider-option\" onclick=\"event.preventDefault(); bulkTogglePredefined(!this.querySelector('input').checked, '" + s.scope +
      "')\">" + "<input type=\"checkbox\" class=\"scope-toggle\" data-scope=\"" + s.scope + "\" data-total=\"" + String(s.total) + "\"" + checked + "> " +
      s.label + "<span class=\"quick-action-count\" data-scope=\"" + s.scope + "\">" + String(s.count) + " of " + String(s.total) + " enabled</span></label>");
  }

  lines.push("<div class=\"dropdown-divider\"></div>");

  // Bulk assign items — one per provider. Items whose tag is filtered out are hidden so updateBulkAssignOptions() can toggle them when the filter changes.
  const allTags = getAllProviderTags();
  const enabled = getEnabledProviders();
  const hasFilter = enabled.length > 0;

  for(const tagInfo of allTags) {

    const hidden = (hasFilter && !enabled.includes(tagInfo.tag) && (tagInfo.tag !== "direct")) ? " style=\"display: none;\"" : "";

    lines.push("<div class=\"dropdown-item bulk-assign-item\" data-provider-tag=\"" + escapeHtml(tagInfo.tag) + "\"" + hidden +
      " onclick=\"closeDropdowns(); bulkAssignProvider('" + escapeHtml(tagInfo.tag) + "')\">Set all to " + escapeHtml(tagInfo.displayName) + "</div>");
  }

  lines.push("</div>");
  lines.push("</div>");
  lines.push("</div>");
  lines.push("</div>");

  const totalCount = listing.length;
  const userCount = listing.filter((entry) => entry.source !== "predefined").length;
  const predefinedCount = totalCount - userCount;
  const enabledCount = totalCount - totalHiddenCount;

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

  // Provider filter toolbar with multi-select dropdown and chips. Placed after the add channel form so the form flows directly from its trigger button.
  lines.push(generateProviderFilterToolbar());

  // Profile reference section (hidden by default, toggled via link in profile dropdown hint).
  lines.push(generateProfileReference(profiles));

  // Channels table. Disabled predefined channels are hidden by default and revealed via the "Show disabled" toggle. The wrapper div enables horizontal scrolling on
  // small screens. Table classes dynamically include hide-col-* for each hidden optional column.
  const visibleCols = new Set(CONFIG.channels.visibleColumns);
  const tableClasses = [ "channel-table", "hide-disabled" ];

  for(const col of OPTIONAL_COLUMNS) {

    if(!visibleCols.has(col.field)) {

      tableClasses.push("hide-" + col.cssClass);
    }
  }

  const sortField = CONFIG.channels.channelSortField;
  const sortDir = CONFIG.channels.channelSortDirection;

  // Channel summary line with predefined/user breakdown. The user-count span contains the entire user portion (comma, count, and label) so the client can
  // toggle it by setting textContent. When there are no user channels, the span is empty to avoid "0 user" noise.
  const userPortion = (userCount > 0) ? ", " + String(userCount) + " user" : "";

  lines.push("<div class=\"channel-summary\"><span id=\"total-count\">" + String(totalCount) + "</span> channels " +
    "(<span id=\"predefined-count\">" + String(predefinedCount) + "</span> predefined<span id=\"user-count\">" + userPortion + "</span>) &middot; " +
    "<span id=\"enabled-count\">" + String(enabledCount) + "</span> enabled &middot; " +
    "<span id=\"disabled-count\">" + String(totalHiddenCount) + "</span> disabled</div>");
  lines.push("<div class=\"channel-table-wrapper\">");
  lines.push("<table class=\"" + tableClasses.join(" ") + "\" data-sort-field=\"" + sortField + "\" data-sort-dir=\"" + sortDir + "\">");
  lines.push("<thead>");
  lines.push("<tr>");

  // Sortable column headers. All columns except Actions are sortable. The active sort column gets a direction indicator triangle.
  const sortableHeaders: { cssClass: string; field: string; label: string }[] = [

    { cssClass: "col-key", field: "key", label: "Key" },
    { cssClass: "col-name", field: "name", label: "Name" },
    { cssClass: "col-provider", field: "provider", label: "Provider" },
    ...OPTIONAL_COLUMNS
  ];

  for(const hdr of sortableHeaders) {

    const isActive = (sortField === hdr.field);
    const indicator = isActive ? ((sortDir === "asc") ? " &#9650;" : " &#9660;") : "";

    lines.push("<th class=\"" + hdr.cssClass + " sortable\" data-sort-field=\"" + hdr.field + "\" onclick=\"sortChannelTable('" + hdr.field + "')\">" +
      hdr.label + indicator + "</th>");
  }

  // Actions header with table options dropdown.
  lines.push("<th class=\"col-actions\"><span>Actions</span>");
  lines.push("<div class=\"dropdown column-picker\">");
  lines.push("<button type=\"button\" class=\"btn-icon btn-col-picker\" title=\"Table options\" aria-label=\"Table options\" " +
    "onclick=\"toggleDropdown(this)\">&#8942;</button>");
  lines.push("<div class=\"dropdown-menu column-picker-menu\">");
  lines.push("<label class=\"provider-option\"><input type=\"checkbox\" id=\"show-disabled-toggle\" onchange=\"toggleDisabledVisibility()\"> " +
    "Show disabled channels</label>");
  lines.push("<div class=\"dropdown-divider\"></div>");

  for(const col of OPTIONAL_COLUMNS) {

    const checked = visibleCols.has(col.field) ? " checked" : "";

    lines.push("<label class=\"provider-option\"><input type=\"checkbox\" data-col-class=\"" + col.cssClass + "\" data-col-field=\"" + col.field +
      "\" onchange=\"toggleColumn(this)\"" + checked + "> " + col.label + "</label>");
  }

  lines.push("</div>");
  lines.push("</div>");
  lines.push("</th>");

  lines.push("</tr>");
  lines.push("</thead>");
  lines.push("<tbody>");

  // Sort the listing by the user's preferred field and direction before rendering rows. The canonical getChannelListing() order is preserved for other callers.
  const sortedListing = [...listing].sort((a, b) => compareChannelSort(a.channel, a.key, b.channel, b.key, sortField, sortDir));

  // Generate rows for all channels using the shared row generator.
  for(const entry of sortedListing) {

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
