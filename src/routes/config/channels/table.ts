/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * table.ts: Channel table rendering for the PrismCast configuration interface.
 */
import { ICON_BOLT, ICON_COPY, ICON_DELETE, ICON_DISABLE, ICON_EDIT, ICON_ENABLE, ICON_FILTER, ICON_HEALTH, ICON_LINK, ICON_LOGIN, ICON_MANAGE,
  ICON_REVERT, ICON_TRANSFER } from "../../icons.js";
import { compareChannelSort, getAllServiceTags, getAuthDomainForChannel, getChannelServiceLabel, getChannelServiceTags, getChannelSortKey,
  getEnabledServices, getPredefinedDomainMap, getServiceGroup, hasMultipleServices, isChannelAvailableByService, isServiceTagEnabled,
  resolvePredefinedVariant, resolveServiceKey } from "../../../config/services.js";
import { escapeHtml, formatTimeAgo } from "../../../utils/index.js";
import { getActiveTagVocabulary, getChannelEffectiveTags, getChannelListing, getChannelLogo, getChannelsParseErrorMessage,
  getPredefinedScopeCounts, getTagRegistry, getUserChannelsFilePath, hasChannelsParseError, isPredefinedChannel, isPredefinedChannelDisabled,
  isUserChannel } from "../../../config/userChannels.js";
import { getCachedProviderChannels, getProviderDomainMap, getProviderGuideUrls, getProviderModuleInfo } from "../../../browser/channelSelection.js";
import { getChannelHealth, getDomainAuth } from "../../../config/health.js";
import { getProfileForChannel, getProfiles } from "../../../config/profiles.js";
import { CONFIG } from "../../../config/index.js";
import type { ChannelListingEntry } from "../../../types/index.js";
import { PREDEFINED_CHANNELS } from "../../../channels/index.js";
import type { ProfileInfo } from "../../../config/profiles.js";
import { categorizeProfiles } from "../index.js";
import { generateWizardModal } from "../../components.js";

/**
 * Generates an annotated service display span. The client-side page-load script processes these elements via serviceIconHtml, rendering the appropriate
 * icon + text combination. The server just emits the text with data attributes — all icon rendering is client-side through the single serviceIconHtml path.
 * @param name - The service display name.
 * @param domain - The service's domain for icon fallback derivation. Undefined for services without a known domain.
 * @param iconUrl - Optional explicit icon URL to try before domain-derived fallbacks.
 * @param small - When true, uses the small icon variant for chips.
 * @returns HTML span string with data attributes for client-side processing.
 */
function serviceDisplaySpan(name: string, domain?: string, iconUrl?: string, small?: boolean): string {

  const domainAttr = domain ? " data-domain=\"" + escapeHtml(domain) + "\"" : "";
  const iconAttr = iconUrl ? " data-icon-url=\"" + escapeHtml(iconUrl) + "\"" : "";
  const sizeAttr = small ? " data-sm" : "";

  return "<span class=\"provider-display\"" + domainAttr + iconAttr + sizeAttr + ">" + escapeHtml(name) + "</span>";
}


// Optional column definitions for the channels table. These columns can be shown or hidden by the user via the column picker. The order here determines the
// column order in the table, slotted between Service and Actions.
export const OPTIONAL_COLUMNS: readonly {
  readonly align: string; readonly cssClass: string; readonly field: string; readonly label: string; readonly width: string;
}[] = [

  { align: "center", cssClass: "col-chnum", field: "channelNumber", label: "Number", width: "70px" },
  { align: "center", cssClass: "col-hdhr", field: "hdhrEnabled", label: "HDHR", width: "55px" },
  { align: "center", cssClass: "col-stationid", field: "stationId", label: "Station ID", width: "100px" },
  { align: "left", cssClass: "col-profile", field: "profile", label: "Profile", width: "130px" },
  { align: "left", cssClass: "col-selector", field: "channelSelector", label: "Selector", width: "130px" },
  { align: "left", cssClass: "col-tags", field: "tags", label: "Tags", width: "150px" }
];

// Total number of columns in the channels table (4 required + 6 optional).
const TOTAL_COLUMN_COUNT = 10;

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
    lines.push("<p class=\"category-desc\">User-defined profiles created via the profile builder wizard or imported from service packs.</p>");
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
/**
 * Options for the advanced fields section of the channel add/edit form.
 */
interface AdvancedFieldOptions {

  // Current channel number value (empty string for none).
  channelNumberValue?: string;

  // Current channel selector value (empty string for none).
  channelSelectorValue?: string;

  // Whether the channel is included in the HDHomeRun/Plex lineup. Defaults to true.
  hdhrEnabled?: boolean;

  // Whether to display hint text beneath each field. Defaults to true.
  showHints?: boolean;

  // Current station ID value (empty string for none).
  stationIdValue?: string;

  // Current tags as a comma-separated string (empty string for none).
  tagsValue?: string;
}

function generateAdvancedFields(idPrefix: string, options: AdvancedFieldOptions = {}): string[] {

  const { channelNumberValue = "", channelSelectorValue = "", hdhrEnabled = true, showHints = true, stationIdValue = "", tagsValue = "" } = options;

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

  // Channel selector. The guide-based service list is derived from the provider module registry so it stays current as services are added.
  const guideProviderNames = getProviderModuleInfo().map((p) => p.label).sort().join(", ");
  const channelSelectorHint = showHints ?
    "Identifies which channel to select on sites that host multiple live streams. Known values are suggested when the URL matches a supported site. " +
    "For guide-based profiles (" + guideProviderNames + "), use the channel name or station code from the guide. " +
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

  // Tags field: checkbox grid of the active tag vocabulary. Checked tags are collected into a hidden input as a comma-separated value on form submission.
  // This prevents users from entering tag names that don't exist in the managed vocabulary.
  const vocabulary = getActiveTagVocabulary();
  const currentTags = new Set(tagsValue ? tagsValue.split(",").map((t) => t.trim().toLowerCase()).filter((t) => t.length > 0) : []);

  if(vocabulary.length > 0) {

    lines.push("<div class=\"form-row\">");
    lines.push("<label>Tags</label>");
    lines.push("<input type=\"hidden\" name=\"tags\" id=\"" + idPrefix + "-tags-hidden\" value=\"" + escapeHtml(tagsValue) + "\">");
    lines.push("<div class=\"tag-checkbox-grid\">");

    for(const tag of vocabulary) {

      const checked = currentTags.has(tag) ? " checked" : "";

      lines.push("<label class=\"tag-checkbox-label\">" +
        "<input type=\"checkbox\" class=\"tag-checkbox\" data-tag=\"" + escapeHtml(tag) + "\"" + checked +
        " onchange=\"updateTagsHidden(this.closest('.form-row'))\">" +
        "<span class=\"tag-badge\">" + escapeHtml(tag) + "</span></label>");
    }

    lines.push("</div>");

    if(showHints) {

      lines.push("<div class=\"hint\">Select tags for playlist filtering. Manage available tags via Manage Channels &gt; Manage Tags.</div>");
    }

    lines.push("</div>");
  }

  // HDHomeRun lineup inclusion. A hidden input provides the "false" value when the checkbox is unchecked (unchecked checkboxes are not submitted in FormData).
  // When checked, the checkbox value "true" overwrites the hidden input's "false" since it appears later in DOM order.
  const hdhrChecked = hdhrEnabled ? " checked" : "";
  const hdhrHint = showHints ? "When unchecked, this channel is hidden from the HDHomeRun lineup and not available in Plex." : undefined;

  lines.push("<div class=\"form-row form-row-checkbox\">");
  lines.push("<label for=\"" + idPrefix + "-hdhrEnabled\">Include in HDHomeRun/Plex Lineup</label>");
  lines.push("<input type=\"hidden\" name=\"hdhrEnabled\" value=\"false\">");
  lines.push("<input type=\"checkbox\" id=\"" + idPrefix + "-hdhrEnabled\" name=\"hdhrEnabled\" value=\"true\"" + hdhrChecked + ">");
  lines.push("</div>");

  if(hdhrHint) {

    lines.push("<div class=\"hint\">" + hdhrHint + "</div>");
  }

  lines.push("</div>"); // End advanced fields.

  return lines;
}

/**
 * Generates JavaScript variables for channel selector datalist population. Produces three variables: `channelSelectorsByDomain` maps URL hostnames to known
 * channel selector values (from predefined channels and cached service discovery), `serviceByDomain` maps service guide URL hostnames to service slugs for
 * client-side async discovery, and `serviceGuideUrl` maps service slugs to their guide URLs for URL correction hints. Embedded as a `<script>` block in the
 * channels panel.
 *
 * @returns JavaScript variable declarations ready to embed in a `<script>` tag.
 */
function generateChannelSelectorData(): string {

  const byDomain: Record<string, { label: string; stationId?: string; value: string }[]> = {};
  const seen: Record<string, Set<string>> = {};

  for(const [ key, raw ] of Object.entries(PREDEFINED_CHANNELS)) {

    if(!raw.channelSelector) {

      continue;
    }

    // Use the resolved channel (with inheritance from the canonical) so stationId, name, and other inherited fields are populated correctly for variants.
    // The channelSelector is captured from the raw entry (already validated as non-null) to avoid non-null assertions on the resolved channel's optional field.
    const selector = raw.channelSelector;
    const channel = resolvePredefinedVariant(key) ?? raw;
    const hostname = new URL(channel.url).hostname;

    seen[hostname] ??= new Set();

    if(seen[hostname].has(selector)) {

      continue;
    }

    seen[hostname].add(selector);
    byDomain[hostname] ??= [];
    byDomain[hostname].push({ label: channel.name ?? selector, stationId: channel.stationId, value: selector });
  }

  // Merge cached service-discovered channels into the domain map. Predefined entries take precedence — we only add discovered channels whose channelSelector
  // value is not already present for that domain. This enriches the datalist with the full service lineup when precaching or prior discovery has run.
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

  // Build the hostname→slug map and slug→guideUrl map for all services (including those with cold caches) so the client-side fetch can trigger discovery for any
  // service domain and the URL hint can suggest the correct guide URL.
  const serviceByDomain = getProviderDomainMap();
  const serviceGuideUrl = getProviderGuideUrls();

  // Sort entries within each domain alphabetically by label for consistent ordering in the datalist dropdown.
  for(const entries of Object.values(byDomain)) {

    entries.sort((a, b) => a.label.localeCompare(b.label));
  }

  return "var channelSelectorsByDomain = " + JSON.stringify(byDomain) + ";\n" +
    "var serviceByDomain = " + JSON.stringify(serviceByDomain) + ";\n" +
    "var serviceGuideUrl = " + JSON.stringify(serviceGuideUrl) + ";\n" +
    "var predefinedByDomain = " + JSON.stringify(getPredefinedDomainMap()) + ";";
}

/**
 * Generates the tag column filter dropdown content. This is the single source of truth for the filter checkbox list — used for both the initial header render
 * and for incremental updates after tag CRUD operations. The dropdown shell (button, dropdown-menu wrapper) is rendered by the header generator; this function
 * provides only the inner content (checkbox labels + "Show All" item).
 * @returns HTML string for the filter dropdown content.
 */
export function generateTagFilterContent(): string {

  const vocabulary = getActiveTagVocabulary();
  const lines: string[] = [];

  for(const tag of vocabulary) {

    lines.push("<label class=\"provider-option\" onclick=\"event.stopPropagation()\">" +
      "<input type=\"checkbox\" class=\"tag-filter-checkbox\" data-tag=\"" + escapeHtml(tag) + "\" checked onchange=\"applyTagColumnFilter()\"> " +
      escapeHtml(tag) + "</label>");
  }

  if(vocabulary.length > 0) {

    lines.push("<div class=\"dropdown-divider\"></div>");
    lines.push("<div class=\"dropdown-item\" id=\"tag-filter-toggle\" onclick=\"event.stopPropagation(); toggleTagColumnFilter()\">Show None</div>");
  }

  return lines.join("");
}

/**
 * Generates the Tag Management modal body HTML. This is the single source of truth for tag manager content — used for both the initial server render and for
 * incremental updates after tag CRUD operations. The endpoints return this HTML in the response so the client can replace the modal content without a page reload.
 * @returns HTML string for the tag manager body (tag list, input field, deleted tags section).
 */
export function generateTagManagerBody(): string {

  const vocabulary = getActiveTagVocabulary();
  const registry = getTagRegistry();

  // Build the tag list HTML. Each tag is a badge with a delete button and inline rename on click.
  const tagListItems: string[] = [];

  for(const tag of vocabulary) {

    tagListItems.push("<div class=\"tag-manager-item\" data-tag=\"" + escapeHtml(tag) + "\">" +
      "<span class=\"tag-badge tag-editable\" title=\"Click to rename\" onclick=\"startTagRename(this, '" + escapeHtml(tag) +
      "')\">" + escapeHtml(tag) + "</span>" +
      "<button type=\"button\" class=\"btn-icon btn-icon-delete\" title=\"Delete tag\" onclick=\"deleteTag('" + escapeHtml(tag) +
      "')\">" + ICON_DELETE + "</button></div>");
  }

  // Deleted predefined tags section — show restore option for tags the user has deleted.
  const deletedItems: string[] = [];

  for(const tag of registry.deletedTags) {

    deletedItems.push("<div class=\"tag-manager-item tag-deleted\" data-tag=\"" + escapeHtml(tag) + "\">" +
      "<span class=\"tag-badge tag-badge-deleted\">" + escapeHtml(tag) + "</span> <span class=\"tag-annotation\">(deleted)</span>" +
      "<button type=\"button\" class=\"btn-icon\" title=\"Restore tag\" onclick=\"restoreTag('" + escapeHtml(tag) +
      "')\">" + ICON_REVERT + "</button></div>");
  }

  return "<div class=\"tag-manager\">" +
    "<p class=\"wizard-hint\">Create and manage the tag vocabulary. Tags can be assigned to channels via the channel table " +
    "for playlist filtering and organization.</p>" +
    "<div class=\"tag-manager-add\">" +
    "<input type=\"text\" id=\"tag-manager-input\" placeholder=\"New tag name\" maxlength=\"30\" " +
    "pattern=\"[a-z0-9]([a-z0-9-]*[a-z0-9])?\" onkeydown=\"if(event.key==='Enter'){event.preventDefault();createTag();}\">" +
    "<button type=\"button\" class=\"btn btn-primary btn-sm\" onclick=\"createTag()\">Add</button>" +
    "</div>" +
    "<div id=\"tag-manager-error\" class=\"wizard-error\" style=\"display: none;\"></div>" +
    "<div id=\"tag-manager-list\" class=\"tag-manager-list\">" +
    (tagListItems.length > 0 ? tagListItems.join("") : "<div class=\"empty-state-text\">No tags defined.</div>") +
    "</div>" +
    (deletedItems.length > 0 ? "<div class=\"tag-manager-section-label\">Deleted Predefined Tags</div>" +
      "<div id=\"tag-manager-deleted\" class=\"tag-manager-list\">" + deletedItems.join("") + "</div>" : "") +
    "</div>";
}

/**
 * Generates the Tag Management modal shell using the wizard modal infrastructure for consistent styling.
 * @returns HTML string for the complete tag management modal.
 */
function generateTagManagementModal(): string {

  return generateWizardModal({

    body: generateTagManagerBody(),
    buttons: [
      { label: "Done", onclick: "closeTagManager()", position: "right", variant: "primary" }
    ],
    id: "tag-manager-modal",
    maxWidth: "420px",
    onClose: "closeTagManager()",
    title: "Manage Tags"
  });
}

/**
 * Generates the Browse Channels 2-step wizard modal. Step 1 is the service picker grid, step 2 is channel discovery and management. Channel state detection
 * (new, switch, current) is handled server-side by the annotated discovery response - no client-side matching logic is needed. Service and guide URL data are
 * embedded as JSON data blocks for the client-side wizard controller.
 * @returns HTML string for the browse modal.
 */
function generateBrowseModal(): string {

  // Build the service info array from the provider module registry. The client uses this to render the service picker.
  const providers = getProviderModuleInfo();
  const guideUrls = getProviderGuideUrls();

  return generateWizardModal({

    buttons: [
      { id: "browse-back", label: "Back", position: "left", role: "back", visible: false },
      { label: "Cancel", position: "right", role: "close" },
      { id: "browse-add-btn", label: "Apply", onclick: "submitBrowseChannels()", position: "right", variant: "primary", visible: false }
    ],
    contentId: "browse-content",
    dataBlocks: [
      "<script type=\"application/json\" id=\"browse-services-data\">" + JSON.stringify(providers) + "</script>",
      "<script type=\"application/json\" id=\"browse-guide-urls-data\">" + JSON.stringify(guideUrls) + "</script>"
    ],
    description: "Manage your channel lineup by service. Add new channels, switch services for existing channels, or remove channels you no longer need.",
    id: "browse-modal",
    steps: [ "Service", "Channels" ],
    title: "Browse Channels",
    titleId: "browse-title"
  });
}

/**
 * Generates the Service Setup 3-step wizard modal. Steps: Services (multi-select), Sign In (sequential auth), Channels (summary + finish). The
 * setupCompleted flag is embedded as a data attribute so the client can auto-show the wizard on first visit.
 * @returns HTML string for the setup wizard modal.
 */
function generateSetupWizardModal(): string {

  const enabled = getEnabledServices();

  // Build service data with enabled state from the current service filter. getAllServiceTags is the single source of truth for service metadata
  // (domain, iconUrl, displayName). The enabled field lets the client pre-check services on re-run without reading DOM state.
  const tags = getAllServiceTags()
    .filter((t) => t.tag !== "direct")
    .map((t) => ({ ...t, enabled: enabled.includes(t.tag) }));

  return generateWizardModal({

    buttons: [
      { id: "setup-back", label: "Back", position: "left", role: "back", visible: false },
      { id: "setup-skip", label: "Skip Setup", onclick: "skipSetup()", position: "right" },
      { id: "setup-next", label: "Next", position: "right", role: "next", variant: "primary" },
      { id: "setup-finish", label: "Finish", onclick: "finishSetup()", position: "right", variant: "primary", visible: false }
    ],
    contentId: "setup-content",
    dataAttributes: { "setup-completed": CONFIG.channels.setupCompleted ? "true" : "false" },
    dataBlocks: [
      "<script type=\"application/json\" id=\"setup-services-data\">" + JSON.stringify(tags) + "</script>"
    ],
    description: "Set up your streaming services in three steps: select your services, sign in, and choose your channels.",
    errorId: "setup-error",
    id: "setup-modal",
    steps: [ "Services", "Sign In", "Channels" ],
    stepsId: "setup-steps",
    title: "Service Setup",
    titleId: "setup-title"
  });
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

  const isUser = isUserChannel(key);
  const isPredefined = isPredefinedChannel(key);
  const isOverride = isPredefined && isUser;
  const isDisabled = isPredefinedChannelDisabled(key);
  const isAvailableByService = isChannelAvailableByService(key);

  // Check if this channel has multiple services.
  const serviceGroup = getServiceGroup(key);

  // Build the service tags data attribute for client-side filtering.
  const serviceTags = getChannelServiceTags(key).join(",");

  // Generate display row. User channels get one CSS class, disabled predefined get another, service-filtered get a third.
  const displayLines: string[] = [];
  const rowClasses: string[] = [];

  if(isUser) {

    rowClasses.push("user-channel");
  }

  if(isDisabled) {

    rowClasses.push("channel-disabled");
  }

  if(!isAvailableByService) {

    rowClasses.push("channel-unavailable");
  }

  const rowClassAttr = (rowClasses.length > 0) ? " class=\"" + rowClasses.join(" ") + "\"" : "";

  // Compute effective tags early — used for both the row data attribute (tag column filter) and the Tags column cell rendering below.
  const effectiveTags = getChannelEffectiveTags(channel);
  const channelTagsAttr = (effectiveTags.length > 0) ? " data-channel-tags=\"" + escapeHtml(effectiveTags.join(",")) + "\"" : "";

  displayLines.push("<tr id=\"display-row-" + escapeHtml(key) + "\"" + rowClassAttr + " data-provider-tags=\"" + escapeHtml(serviceTags) +
    "\"" + channelTagsAttr + ">");
  displayLines.push("<td class=\"ch-key\" data-sort-value=\"" + escapeHtml(getChannelSortKey(channel, key, "key")) + "\">" + escapeHtml(key) + "</td>");
  const channelLogoUrl = getChannelLogo(key) ?? "";
  const logoAttr = channelLogoUrl ? " data-logo=\"" + escapeHtml(channelLogoUrl) + "\"" : "";

  displayLines.push("<td data-sort-value=\"" + escapeHtml(getChannelSortKey(channel, key, "name")) + "\"" + logoAttr +
    "><span class=\"channel-name-cell\">" + escapeHtml(channel.name ?? key) + "</span></td>");

  // Service column: dropdown for multi-service channels, static service name for single-service. Both states always render a hidden "No available services"
  // label alongside the service content so that client-side filterChannelRows() can toggle between them without a page reload.
  displayLines.push("<td data-sort-value=\"" + escapeHtml(getChannelSortKey(channel, key, "service")) + "\">");

  const labelHidden = isAvailableByService ? " style=\"display:none\"" : "";
  const contentHidden = isAvailableByService ? "" : " style=\"display:none\"";

  displayLines.push("<em class=\"no-provider-label\"" + labelHidden + ">No available services</em>");

  if(hasMultipleServices(key) && serviceGroup) {

    // Multi-service: render ALL variants with data-provider-tag attributes so client-side JS can filter options when the service selection changes. Filtered-out
    // options get the hidden attribute for immediate filtering in Chrome. Safari ignores hidden on option elements, so the page-load JS init calls filterChannelRows()
    // to remove them from the DOM.
    const currentSelection = resolveServiceKey(key);

    displayLines.push("<select class=\"provider-select\" data-channel=\"" + escapeHtml(key) +
      "\" title=\"Choose which streaming service delivers this channel\" onchange=\"updateServiceSelection(this)\"" + contentHidden + ">");

    for(const variant of serviceGroup.variants) {

      const selected = (variant.key === currentSelection) ? " selected" : "";
      const optionHidden = !isServiceTagEnabled(variant.tag) ? " hidden" : "";

      displayLines.push("<option value=\"" + escapeHtml(variant.key) + "\" data-provider-tag=\"" + escapeHtml(variant.tag) + "\"" + selected + optionHidden + ">" +
        escapeHtml(variant.label) + "</option>");
    }

    displayLines.push("</select>");
  } else {

    // Single-service: wrap the service name in a span so client-side JS can toggle it with the no-service label. Uses profile-aware label resolution so
    // channels with explicit profile assignments show the profile's service name rather than the built-in name for the URL domain.
    displayLines.push("<span class=\"provider-name\"" + contentHidden + ">" +
      escapeHtml(getChannelServiceLabel(channel)) + "</span>");
  }

  displayLines.push("</td>");

  // Optional columns: Number, HDHR, Station ID, Profile, Selector. All five are always rendered; visibility is controlled by CSS classes on the table element.
  const cellKey = escapeHtml(key);

  displayLines.push("<td class=\"col-chnum editable-cell\" data-sort-value=\"" + escapeHtml(getChannelSortKey(channel, key, "channelNumber")) +
    "\" data-field=\"channelNumber\" data-key=\"" + cellKey + "\" data-value=\"" + (channel.channelNumber ? escapeHtml(String(channel.channelNumber)) : "") +
    "\" onclick=\"startInlineEdit(this)\">" + (channel.channelNumber ? escapeHtml(String(channel.channelNumber)) : "<span class=\"text-muted\">&ndash;</span>") +
    "</td>");

  // HDHR column: inline checkbox for quick toggling. The checkbox submits changes via the toggleHdhr() client-side function.
  const hdhrCheckedAttr = (channel.hdhrEnabled !== false) ? " checked" : "";

  displayLines.push("<td class=\"col-hdhr\" data-sort-value=\"" + escapeHtml(getChannelSortKey(channel, key, "hdhrEnabled")) +
    "\"><input type=\"checkbox\" data-key=\"" + cellKey + "\"" + hdhrCheckedAttr +
    " onchange=\"toggleHdhr(this)\" title=\"Include in HDHomeRun/Plex lineup\"></td>");

  displayLines.push("<td class=\"col-stationid editable-cell\" data-sort-value=\"" + escapeHtml(getChannelSortKey(channel, key, "stationId")) +
    "\" data-field=\"stationId\" data-key=\"" + cellKey + "\" data-value=\"" + (channel.stationId ? escapeHtml(channel.stationId) : "") +
    "\" onclick=\"startInlineEdit(this)\">" + (channel.stationId ? escapeHtml(channel.stationId) : "<span class=\"text-muted\">&ndash;</span>") + "</td>");

  // Profile column: show explicit profile as-is, or the auto-resolved friendly name with "(auto)" suffix in muted style.
  const profileSortKey = escapeHtml(getChannelSortKey(channel, key, "profile"));

  if(channel.profile) {

    displayLines.push("<td class=\"col-profile\" data-sort-value=\"" + profileSortKey + "\">" + escapeHtml(channel.profile) + "</td>");
  } else {

    const resolved = getProfileForChannel(channel);

    if(resolved.profileName !== "default") {

      const label = getChannelServiceLabel(channel);

      displayLines.push("<td class=\"col-profile\" data-sort-value=\"" + profileSortKey +
        "\"><span class=\"text-muted\">" + escapeHtml(label + " (auto)") + "</span></td>");
    } else {

      displayLines.push("<td class=\"col-profile\" data-sort-value=\"" + profileSortKey + "\"></td>");
    }
  }

  displayLines.push("<td class=\"col-selector\" data-sort-value=\"" + escapeHtml(getChannelSortKey(channel, key, "channelSelector")) + "\">" +
    (channel.channelSelector ? escapeHtml(channel.channelSelector) : "<span class=\"text-muted\">&ndash;</span>") + "</td>");

  // Tags column: render effective tags as pills. Clicking the cell opens a shared portal dropdown (rendered in <body>, positioned via getBoundingClientRect)
  // for inline tag editing. The cell carries data-key and data-tags so the dropdown can populate the correct checked state.
  const tagsHtml = (effectiveTags.length > 0) ?
    effectiveTags.map((tag) => "<span class=\"tag-badge\">" + escapeHtml(tag) + "</span>").join(" ") :
    "<span class=\"text-muted\">&ndash;</span>";

  displayLines.push("<td class=\"col-tags editable-cell dropdown\" data-sort-value=\"" + escapeHtml(getChannelSortKey(channel, key, "tags")) +
    "\" data-key=\"" + cellKey + "\" data-tags=\"" + escapeHtml(effectiveTags.join(",")) +
    "\" onclick=\"toggleInlineTagDropdown(this)\">" + tagsHtml + "</td>");

  // Actions column with icon buttons. Five positions per row: Edit (always), Login/placeholder, Health/placeholder, context-sensitive, and Copy URL.
  displayLines.push("<td>");
  displayLines.push("<div class=\"btn-group\">");

  const escapedKey = escapeHtml(key);

  // Resolve the auth domain for the currently selected service variant. Used for both login icon color and health icon lookups.
  const variantKey = resolveServiceKey(key);
  const authDomain = getAuthDomainForChannel(variantKey);

  // Position 1: Edit (all channels).
  displayLines.push("<button type=\"button\" class=\"btn-icon btn-icon-edit\" title=\"Edit\" aria-label=\"Edit\" onclick=\"showEditForm('" + escapedKey +
    "')\">" + ICON_EDIT + "</button>");

  // Position 2: Login for enabled channels (with domain auth color), placeholder for disabled predefined. Custom channels (user-defined, not predefined) skip
  // login coloring because they have no service concept.
  if(!isDisabled) {

    const authTimestamp = (isPredefined || isOverride) ? getDomainAuth(authDomain) : null;
    const loginColorClass = authTimestamp ? " health-success" : "";
    const loginTitle = authTimestamp ? "Verified " + formatTimeAgo(authTimestamp) : "Not yet verified";

    displayLines.push("<button type=\"button\" class=\"btn-icon btn-icon-login" + loginColorClass + "\" data-auth-domain=\"" +
      escapeHtml(authDomain) + "\" title=\"" + loginTitle + "\" aria-label=\"Login\" " +
      "onclick=\"startChannelLogin('" + escapedKey + "')\">" + ICON_LOGIN + "</button>");
  } else {

    displayLines.push("<span class=\"btn-icon-placeholder\"></span>");
  }

  // Position 3: Channel health indicator. Shows last tune result via color. Non-interactive (span, not button). Disabled channels get a placeholder instead.
  if(!isDisabled) {

    const channelHealthResult = getChannelHealth(key, authDomain);
    const healthColorClass = (channelHealthResult?.status === "success") ? " health-success" : (channelHealthResult?.status === "failed") ? " health-failed" : "";
    const healthTitle = channelHealthResult ?
      (channelHealthResult.status === "success" ? "Succeeded " : "Failed ") + formatTimeAgo(channelHealthResult.timestamp) : "Not yet tuned";

    displayLines.push("<span class=\"btn-icon btn-icon-health" + healthColorClass + "\" title=\"" + healthTitle +
      "\" aria-label=\"Channel health\">" + ICON_HEALTH + "</span>");
  } else {

    displayLines.push("<span class=\"btn-icon-placeholder\"></span>");
  }

  // Position 4: enable/disable toggle for predefined channels (regardless of override state), delete for user-defined channels. The enable/disable state and
  // property overrides are independent concerns — a predefined channel can be disabled AND have customizations, and the toggle always reflects the visibility state.
  if(isPredefined) {

    if(isDisabled) {

      displayLines.push("<button type=\"button\" class=\"btn-icon btn-icon-enable\" title=\"Enable\" aria-label=\"Enable\" onclick=\"togglePredefinedChannel('" +
        escapedKey + "', true)\">" + ICON_ENABLE + "</button>");
    } else {

      displayLines.push("<button type=\"button\" class=\"btn-icon btn-icon-disable\" title=\"Disable\" aria-label=\"Disable\" onclick=\"togglePredefinedChannel('" +
        escapedKey + "', false)\">" + ICON_DISABLE + "</button>");
    }
  } else if(isUser) {

    displayLines.push("<button type=\"button\" class=\"btn-icon btn-icon-delete\" title=\"Delete\" aria-label=\"Delete\" onclick=\"deleteChannel('" +
      escapedKey + "')\">" + ICON_DELETE + "</button>");
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

  // Generate edit form row for all channels. Pre-populate with the currently selected service's values so the user sees what they're actually streaming, not the
  // canonical definition which may differ when a service variant is selected.
  const editLines: string[] = [];

  editLines.push("<tr id=\"edit-row-" + escapedKey + "\" style=\"display: none;\">");
  editLines.push("<td colspan=\"" + String(TOTAL_COLUMN_COUNT) + "\">");
  editLines.push("<div class=\"channel-form\" style=\"margin: 0;\">");
  editLines.push("<h3>Edit Channel: " + escapedKey + "</h3>");
  editLines.push("<form id=\"edit-channel-form-" + escapedKey + "\" onsubmit=\"return submitChannelForm(event, 'edit')\">");
  editLines.push("<input type=\"hidden\" name=\"action\" value=\"edit\">");
  editLines.push("<input type=\"hidden\" name=\"key\" value=\"" + escapedKey + "\">");

  // Channel name.
  editLines.push(...generateTextField("edit-name-" + key, "name", "Display Name", channel.name ?? key, {

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
  editLines.push(...generateAdvancedFields("edit-" + key, {

    channelNumberValue: channel.channelNumber ? String(channel.channelNumber) : "",
    channelSelectorValue: channel.channelSelector ?? "",
    hdhrEnabled: channel.hdhrEnabled !== false,
    stationIdValue: channel.stationId ?? "",
    tagsValue: effectiveTags.join(", ")
  }));

  // Form buttons.
  editLines.push("<div class=\"form-buttons\">");
  editLines.push("<button type=\"submit\" class=\"btn btn-primary\">Save Changes</button>");
  editLines.push("<button type=\"button\" class=\"btn btn-secondary\" onclick=\"hideEditForm('" + escapedKey + "')\">Cancel</button>");

  if(isOverride) {

    editLines.push("<button type=\"button\" class=\"btn btn-secondary btn-revert\" onclick=\"revertChannel('" + escapedKey +
      "')\">Revert to Defaults</button>");
  }

  editLines.push("</div>");

  editLines.push("</form>");
  editLines.push("</div>");
  editLines.push("</td>");
  editLines.push("</tr>");

  const editRow = editLines.join("\n");

  return { displayRow, editRow };
}

// Channel table patch system.

/**
 * Channel table state snapshot. Represents the complete set of summary counts for the channel header and scope toggles. Computed server-side as the single
 * source of truth — the client applies these values directly to DOM elements without recalculation.
 */
export interface ChannelTableCounts {

  // Number of predefined channels that are disabled or unavailable by service filter.
  disabled: number;

  // Number of channels that are enabled and available by service filter.
  enabled: number;

  // Number of predefined channels (regardless of disabled state).
  predefined: number;

  // Total number of canonical channels in the listing (predefined + user, excluding service variants).
  total: number;

  // Number of user-defined channels (pure user + overrides of predefined).
  user: number;
}

/**
 * Patch response structure for channel table mutations. Contains everything the client needs to update the UI without a page reload. Mutation endpoints return
 * this alongside their existing success/message fields. The client's applyChannelPatch function applies whichever fields are present.
 */
export interface ChannelTablePatch {

  // Summary counts for the channel header.
  counts: ChannelTableCounts;

  // HDHR bulk toggle counts (enabled channels vs total) for the Quick Actions tri-state checkbox.
  hdhrCounts: { enabled: number; total: number };

  // Channel logos keyed by channel key. Present when logos were resolved during the mutation.
  logos?: Record<string, string>;

  // Affected rows. Each entry specifies the channel key, an action (update or remove), and pre-rendered row HTML for updates.
  rows: { action: "remove" | "update"; displayRow?: string; editRow?: string; key: string }[];

  // Scope toggle counts for Quick Actions (all/east/pacific, each with total and enabled).
  scopeCounts: { all: { enabled: number; total: number }; east: { enabled: number; total: number }; pacific: { enabled: number; total: number } };

  // Tag bulk toggle counts for Quick Actions. Maps each active tag to how many enabled channels have it vs total enabled channels. Omitted when the tags column
  // is not visible (no tag toggles to update).
  tagCounts?: Record<string, { count: number; total: number }>;
}

/**
 * Computes the complete channel table state — summary counts and scope toggle counts — from the current channel listing. This is the server-side single source
 * of truth for all count computation, replacing the client-side DOM-scanning approach. Called once per mutation as part of building a patch.
 * @param listing - Optional pre-fetched listing. When omitted, fetches from getChannelListing(). Passing the listing avoids a redundant computation when
 *   buildChannelTablePatch already has it.
 * @returns The channel table counts and scope toggle counts.
 */
export function buildChannelTableState(listing?: ChannelListingEntry[]): { counts: ChannelTableCounts;
  scopeCounts: { all: { enabled: number; total: number }; east: { enabled: number; total: number }; pacific: { enabled: number; total: number } };
} {

  listing ??= getChannelListing();

  let predefined = 0;
  let user = 0;
  let disabled = 0;

  for(const entry of listing) {

    if((entry.source === "user") || (entry.source === "override")) {

      user++;
    } else {

      predefined++;
    }

    if(!entry.enabled || !entry.availableByService) {

      disabled++;
    }
  }

  const total = listing.length;
  const enabled = total - disabled;

  return {

    counts: { disabled, enabled, predefined, total, user },
    scopeCounts: getPredefinedScopeCounts()
  };
}

/**
 * Counts channels included and excluded from the HDHomeRun lineup. Only counts enabled, service-available channels (the same set visible in the table).
 * @param listing - The channel listing entries.
 * @returns An object with `enabled` (HDHR-included) and `total` counts.
 */
function getHdhrCounts(listing: ChannelListingEntry[]): { enabled: number; total: number } {

  let enabled = 0;
  let total = 0;

  for(const entry of listing) {

    if(!entry.enabled || !entry.availableByService) {

      continue;
    }

    total++;

    if(entry.channel.hdhrEnabled !== false) {

      enabled++;
    }
  }

  return { enabled, total };
}

/**
 * Computes per-tag channel counts for the Quick Actions tag bulk toggles. For each tag in the active vocabulary, counts how many enabled, service-available
 * channels have that tag in their effective tags. Returns undefined when the tags column is not visible (no tag toggles to update).
 * @param listing - The channel listing entries.
 * @returns A record mapping tag names to { count, total }, or undefined when hidden.
 */
function getTagCounts(listing: ChannelListingEntry[]): Record<string, { count: number; total: number }> | undefined {

  const visibleCols = new Set(CONFIG.channels.visibleColumns);

  if(!visibleCols.has("tags")) {

    return undefined;
  }

  const vocabulary = getActiveTagVocabulary();

  if(vocabulary.length === 0) {

    return undefined;
  }

  const result: Record<string, { count: number; total: number }> = {};
  let total = 0;

  // Pre-compute the vocabulary Set once for the per-channel effective tags filter.
  const vocabularySet = new Set(vocabulary);
  const counts = new Map<string, number>();

  for(const tag of vocabulary) {

    counts.set(tag, 0);
  }

  for(const entry of listing) {

    if(!entry.enabled || !entry.availableByService) {

      continue;
    }

    total++;

    const effectiveTags = entry.channel.tags ? entry.channel.tags.filter((tag) => vocabularySet.has(tag)) : [];

    for(const tag of effectiveTags) {

      const current = counts.get(tag);

      if(current !== undefined) {

        counts.set(tag, current + 1);
      }
    }
  }

  for(const tag of vocabulary) {

    result[tag] = { count: counts.get(tag) ?? 0, total };
  }

  return result;
}

/**
 * Builds a complete channel table patch for the given set of affected channel keys. Generates row HTML for keys that exist in the listing (update action) and
 * remove actions for keys that no longer exist. Includes the current summary counts and scope toggle counts. This is the single function mutation endpoints
 * call to construct their patch response.
 * @param affectedKeys - The channel keys that were added, modified, or removed by the mutation.
 * @param profiles - Available profiles for the edit form dropdown.
 * @returns A complete ChannelTablePatch ready for client-side application.
 */
export function buildChannelTablePatch(affectedKeys: string[], profiles: ProfileInfo[]): ChannelTablePatch {

  const listing = getChannelListing();
  const listingByKey = new Map(listing.map((entry) => [ entry.key, entry ]));
  const rows: ChannelTablePatch["rows"] = [];

  for(const key of affectedKeys) {

    const entry = listingByKey.get(key);

    if(entry) {

      const rowHtml = generateChannelRowHtml(key, profiles, entry);

      rows.push({ action: "update", displayRow: rowHtml.displayRow, editRow: rowHtml.editRow, key });
    } else {

      rows.push({ action: "remove", key });
    }
  }

  const { counts, scopeCounts } = buildChannelTableState(listing);

  return { counts, hdhrCounts: getHdhrCounts(listing), rows, scopeCounts, tagCounts: getTagCounts(listing) };
}

/**
 * Generates the service filter toolbar HTML with a multi-select dropdown and dismissable chips.
 * @returns HTML string for the service filter toolbar.
 */
export function generateServiceFilterToolbar(): string {

  const allTags = getAllServiceTags();
  const enabled = getEnabledServices();
  const hasFilter = enabled.length > 0;

  const lines: string[] = [];

  lines.push("<div class=\"provider-toolbar\">");

  // Service filter dropdown and chips. The button follows the toolbar dropdown pattern: icon + label + caret in a single button.
  lines.push("<div class=\"toolbar-group\">");
  lines.push("<div class=\"dropdown provider-dropdown\">");

  const buttonText = hasFilter ? "Filtered" : "All Services";

  lines.push("<button type=\"button\" class=\"btn btn-sm toolbar-icon-btn\" id=\"provider-filter-btn\" " +
    "title=\"Filter channels by streaming service\" onclick=\"toggleDropdown(this)\">" + ICON_FILTER + " " + buttonText + " &#9662;</button>");
  lines.push("<div class=\"dropdown-menu provider-dropdown-menu\">");

  for(const tagInfo of allTags) {

    const isDirectTag = tagInfo.tag === "direct";
    const isChecked = isDirectTag || !hasFilter || enabled.includes(tagInfo.tag);
    const checkedAttr = isChecked ? " checked" : "";
    const disabledAttr = isDirectTag ? " disabled" : "";

    lines.push("<label class=\"provider-option\">");
    lines.push("<input type=\"checkbox\" data-tag=\"" + escapeHtml(tagInfo.tag) + "\"" + checkedAttr + disabledAttr +
      " onchange=\"toggleServiceTag(this)\"> " + serviceDisplaySpan(tagInfo.displayName, tagInfo.domain, tagInfo.iconUrl));
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

      const chipTag = allTags.find((t) => t.tag === tag);
      const displayName = chipTag?.displayName ?? tag;

      lines.push("<span class=\"provider-chip\" data-tag=\"" + escapeHtml(tag) + "\">" +
        serviceDisplaySpan(displayName, chipTag?.domain, chipTag?.iconUrl, true) +
        "<button type=\"button\" class=\"chip-close\" title=\"Remove " + escapeHtml(displayName) + " from filter\" aria-label=\"Remove " +
        escapeHtml(displayName) + "\" onclick=\"removeServiceChip('" + escapeHtml(tag) + "')\">&times;</button></span>");
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

  // Get the canonical channel listing (service variants already filtered out, sorted by key). This is the single source of truth for merged channel data —
  // it handles predefined/user merging, disabled state, and service availability.
  const listing = getChannelListing();
  const profiles = getProfiles();

  // Count channels hidden from the default view: disabled predefined channels OR channels with no available services.
  const totalHiddenCount = listing.filter((entry) => !entry.enabled || !entry.availableByService).length;

  const lines: string[] = [];

  // Panel description.
  lines.push("<div class=\"settings-panel-description\">");
  lines.push("<p>Define and manage streaming channels for the playlist. Customized channels are highlighted.</p>");
  lines.push("<p class=\"description-hint\">Tip: Use the <strong>service filter</strong> above to show only channels from services you subscribe to &mdash; ",
    "this also controls which channels Channels DVR sees in the playlist. Use the <strong>service dropdown</strong> on any multi-service channel to choose ",
    "which streaming service delivers it (e.g., Comedy Central via Hulu vs Sling). Click the <strong>edit icon</strong> to customize any channel's name, ",
    "Gracenote station ID, URL, or other properties.</p>");
  lines.push("</div>");

  // Toolbar with three dropdown menus: Manage Channels (primary actions), Import/Export (data I/O), and Quick Actions (bulk operations). Each dropdown has an
  // inline SVG icon + label + chevron for visual discoverability. Grouped menus reduce visual clutter and separate channel management from data I/O and bulk
  // operations.

  lines.push("<div class=\"channel-toolbar\">");
  lines.push("<div class=\"toolbar-group\">");

  // Manage Channels dropdown — primary channel creation and setup actions.
  lines.push("<div class=\"dropdown\">");
  lines.push("<button type=\"button\" class=\"btn btn-primary btn-sm toolbar-icon-btn\" title=\"Add, browse, or set up channels\" " +
    "onclick=\"toggleDropdown(this)\">" + ICON_MANAGE + " Manage Channels &#9662;</button>");
  lines.push("<div class=\"dropdown-menu\">");
  // Dropdown item icons: tinted variants of the shared icons for visual differentiation in menu items. Each uses a distinct color so items are scannable at
  // a glance. These are local to this function because the tint colors are context-specific to this dropdown.
  const TINTED_ADD = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"#22a563\" stroke-width=\"1.5\" " +
    "stroke-linecap=\"round\" stroke-linejoin=\"round\"><path d=\"M8 3v10M3 8h10\"/></svg>";

  const TINTED_BROWSE = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"#5b8def\" stroke-width=\"1.5\" " +
    "stroke-linecap=\"round\" stroke-linejoin=\"round\"><rect x=\"2\" y=\"2\" width=\"5\" height=\"5\" rx=\"1\"/><rect x=\"9\" y=\"2\" width=\"5\" " +
    "height=\"5\" rx=\"1\"/><rect x=\"2\" y=\"9\" width=\"5\" height=\"5\" rx=\"1\"/><rect x=\"9\" y=\"9\" width=\"5\" height=\"5\" rx=\"1\"/></svg>";

  const TINTED_SETUP = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"#9b8cd8\" stroke-width=\"1.5\" " +
    "stroke-linecap=\"round\" stroke-linejoin=\"round\"><circle cx=\"8\" cy=\"8\" r=\"2.5\"/><path d=\"M8 1.5v2M8 12.5v2M1.5 8h2M12.5 8h2M3.1 " +
    "3.1l1.4 1.4M11.5 11.5l1.4 1.4M3.1 12.9l1.4-1.4M11.5 4.5l1.4-1.4\"/></svg>";

  const TINTED_TAG = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"#9b59b6\" stroke-width=\"1.5\" " +
    "stroke-linecap=\"round\" stroke-linejoin=\"round\"><path d=\"M1.5 8.7V2.5a1 1 0 011-1h6.2a1 1 0 01.7.3l5.1 5.1a1 1 0 010 1.4l-5.5 5.5a1 " +
    "1 0 01-1.4 0L1.8 9.4a1 1 0 01-.3-.7z\"/><circle cx=\"5\" cy=\"5\" r=\"1\"/></svg>";

  lines.push("<div class=\"dropdown-item dropdown-item-icon\" onclick=\"closeDropdowns(); " +
    "document.getElementById('add-channel-form').style.display='block';\">" + TINTED_ADD + " Add Channel</div>");
  lines.push("<div class=\"dropdown-item dropdown-item-icon\" onclick=\"closeDropdowns(); openBrowseModal()\">" + TINTED_BROWSE +
    " Browse Service Channels</div>");
  lines.push("<div class=\"dropdown-item dropdown-item-icon\" onclick=\"closeDropdowns(); openTagManager()\">" + TINTED_TAG +
    " Manage Tags</div>");
  lines.push("<div class=\"dropdown-item dropdown-item-icon\" onclick=\"closeDropdowns(); openSetupWizard()\">" + TINTED_SETUP +
    " Service Setup</div>");
  lines.push("</div>");
  lines.push("</div>");

  // Import / Export dropdown — data I/O operations.
  lines.push("<div class=\"dropdown\">");
  lines.push("<button type=\"button\" class=\"btn btn-secondary btn-sm toolbar-icon-btn\" title=\"Import or export channel data\" " +
    "onclick=\"toggleDropdown(this)\">" + ICON_TRANSFER + " Import / Export &#9662;</button>");
  lines.push("<div class=\"dropdown-menu\">");
  const TINTED_IMPORT = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"#5b8def\" stroke-width=\"1.5\" " +
    "stroke-linecap=\"round\" stroke-linejoin=\"round\"><path d=\"M8 2v8M5 7l3 3 3-3\"/><path d=\"M2 11v2a1 1 0 001 1h10a1 1 0 001-1v-2\"/></svg>";

  const TINTED_EXPORT = "<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"#22a563\" stroke-width=\"1.5\" " +
    "stroke-linecap=\"round\" stroke-linejoin=\"round\"><path d=\"M8 10V2M5 5l3-3 3 3\"/><path d=\"M2 11v2a1 1 0 001 1h10a1 1 0 001-1v-2\"/></svg>";

  lines.push("<div class=\"dropdown-item dropdown-item-icon\" onclick=\"closeDropdowns(); document.getElementById('import-channels-file').click()\">" +
    TINTED_IMPORT + " Import Channels (JSON)</div>");
  lines.push("<div class=\"dropdown-item dropdown-item-icon\" onclick=\"closeDropdowns(); document.getElementById('import-m3u-file').click()\">" +
    TINTED_IMPORT + " Import M3U Playlist</div>");
  lines.push("<label class=\"dropdown-option\"><input type=\"checkbox\" id=\"m3u-replace-duplicates\"> Replace duplicates on M3U import</label>");
  lines.push("<div class=\"dropdown-divider\"></div>");
  lines.push("<div class=\"dropdown-item dropdown-item-icon\" onclick=\"closeDropdowns(); exportChannels()\">" +
    TINTED_EXPORT + " Export Channels (JSON)</div>");
  lines.push("</div>");
  lines.push("</div>");
  lines.push("<input type=\"file\" id=\"import-m3u-file\" accept=\".m3u,.m3u8\" style=\"display: none;\" onchange=\"importM3U(this)\">");

  // Visible column set, used by Quick Actions to gate column-dependent options and by the table header to set initial hide classes.
  const visibleCols = new Set(CONFIG.channels.visibleColumns);

  // Quick Actions dropdown — bulk operations for predefined channels.
  lines.push("<div class=\"dropdown quick-actions-dropdown\">");
  lines.push("<button type=\"button\" class=\"btn btn-secondary btn-sm toolbar-icon-btn\" title=\"Bulk operations for predefined channels\" " +
    "onclick=\"toggleDropdown(this)\">" + ICON_BOLT + " Quick Actions &#9662;</button>");
  lines.push("<div class=\"dropdown-menu\">");

  // Compute initial toggle counts for predefined channel scopes. The server is the single source of truth — the client renders what we return here.
  const scopeCounts = getPredefinedScopeCounts();

  // Three toggle rows: checkbox + label + count. Clicking toggles the group via bulkTogglePredefined(). The onclick uses event.preventDefault() to stop the
  // native checkbox toggle — the server response drives the update.
  const scopes: { count: number; label: string; scope: string; total: number }[] = [
    { count: scopeCounts.all.enabled, label: "All Predefined", scope: "all", total: scopeCounts.all.total },
    { count: scopeCounts.east.enabled, label: "East Variants", scope: "east", total: scopeCounts.east.total },
    { count: scopeCounts.pacific.enabled, label: "Pacific Variants", scope: "pacific", total: scopeCounts.pacific.total }
  ];

  for(const s of scopes) {

    const checked = (s.count === s.total) ? " checked" : "";

    lines.push("<label class=\"provider-option\" onclick=\"event.preventDefault(); bulkTogglePredefined(!this.querySelector('input').checked, '" + s.scope +
      "')\">" + "<input type=\"checkbox\" class=\"scope-toggle\" data-scope=\"" + s.scope + "\"" + checked + "> " +
      s.label + "<span class=\"quick-action-count\" data-scope=\"" + s.scope + "\" data-enabled=\"" + String(s.count) + "\" data-total=\"" + String(s.total) +
      "\">" + String(s.count) + " of " + String(s.total) + " enabled</span></label>");
  }

  lines.push("<div class=\"dropdown-divider\"></div>");

  // Bulk assign select — switch all multi-service channels to a single service.
  const allTags = getAllServiceTags();
  const enabled = getEnabledServices();
  const hasFilter = enabled.length > 0;

  lines.push("<div class=\"bulk-assign-row\">");
  lines.push("<span>Set all channels to:</span>");
  lines.push("<select id=\"bulk-assign-select\" class=\"bulk-assign-select\" title=\"Switch all multi-service channels to this service\" " +
    "onchange=\"if(this.value) { closeDropdowns(); bulkAssignService(this.value); this.value = ''; }\">");
  lines.push("<option value=\"\">Select service</option>");

  for(const tagInfo of allTags) {

    const hidden = (hasFilter && !enabled.includes(tagInfo.tag) && (tagInfo.tag !== "direct")) ? " hidden" : "";

    lines.push("<option value=\"" + escapeHtml(tagInfo.tag) + "\" data-provider-tag=\"" + escapeHtml(tagInfo.tag) + "\"" + hidden + ">" +
      escapeHtml(tagInfo.displayName) + "</option>");
  }

  lines.push("</select>");
  lines.push("</div>");

  // Divider before column-gated quick actions. Hidden when neither the channel number nor HDHR column is visible.
  const dividerVisible = (visibleCols.has("channelNumber") || visibleCols.has("hdhrEnabled")) ? "" : " style=\"display: none;\"";

  lines.push("<div class=\"dropdown-divider\" id=\"quick-action-divider\"" + dividerVisible + "></div>");

  // Auto-number channels — assign sequential channel numbers to visible channels in current sort order. Visible when the channel number column is shown.
  const autoNumberVisible = visibleCols.has("channelNumber") ? "" : " style=\"display: none;\"";

  lines.push("<div id=\"quick-action-autonumber\"" + autoNumberVisible + ">");
  lines.push("<div class=\"bulk-assign-row\">");
  lines.push("<span>Auto-number from:</span>");
  lines.push("<input type=\"number\" id=\"auto-number-start\" class=\"auto-number-input\" value=\"1\" min=\"0\" max=\"99999\" " +
    "placeholder=\"Clear\" onclick=\"event.stopPropagation()\">");
  lines.push("<button type=\"button\" class=\"btn btn-sm btn-secondary\" onclick=\"closeDropdowns(); autoNumberChannels()\">Apply</button>");
  lines.push("</div>");
  lines.push("</div>");

  // HDHR bulk toggle — tri-state checkbox to enable/disable all channels for the HDHomeRun lineup. Visible when the HDHR column is shown.
  const hdhrVisible = visibleCols.has("hdhrEnabled") ? "" : " style=\"display: none;\"";
  const hdhrCounts = getHdhrCounts(listing);
  const hdhrCheckedAttr = (hdhrCounts.enabled === hdhrCounts.total) ? " checked" : "";
  const hdhrIndeterminate = ((hdhrCounts.enabled > 0) && (hdhrCounts.enabled < hdhrCounts.total)) ? " data-indeterminate=\"true\"" : "";

  lines.push("<div id=\"quick-action-hdhr\"" + hdhrVisible + ">");
  lines.push("<label class=\"provider-option\" onclick=\"event.preventDefault(); bulkToggleHdhr()\">");
  lines.push("<input type=\"checkbox\" id=\"hdhr-bulk-toggle\"" + hdhrCheckedAttr + hdhrIndeterminate + "> ");
  lines.push("Include all in HDHR/Plex");
  lines.push("<span class=\"quick-action-count\" id=\"hdhr-bulk-count\">" + String(hdhrCounts.enabled) + " of " + String(hdhrCounts.total) + "</span>");
  lines.push("</label>");
  lines.push("</div>");

  // Bulk tag toggles — tristate checkboxes for each tag in the active vocabulary. Visible when the tags column is shown. Each checkbox shows how many enabled
  // channels have the tag, and clicking it adds or removes the tag on all visible channels.
  const tagsVisible = visibleCols.has("tags") ? "" : " style=\"display: none;\"";
  const tagsVocabulary = getActiveTagVocabulary();

  if(tagsVocabulary.length > 0) {

    // Count how many enabled, service-available channels have each tag.
    const enabledListing = listing.filter((entry) => entry.enabled && entry.availableByService);
    const totalEnabled = enabledListing.length;
    const tagCountMap = new Map<string, number>();

    for(const tag of tagsVocabulary) {

      tagCountMap.set(tag, 0);
    }

    for(const entry of enabledListing) {

      const effectiveTags = getChannelEffectiveTags(entry.channel);

      for(const tag of effectiveTags) {

        const current = tagCountMap.get(tag);

        if(current !== undefined) {

          tagCountMap.set(tag, current + 1);
        }
      }
    }

    lines.push("<div class=\"dropdown-divider\" id=\"quick-action-tags-divider\"" + tagsVisible + "></div>");
    lines.push("<div id=\"quick-action-tags\"" + tagsVisible + ">");
    lines.push("<div class=\"quick-action-section-label\">Tag Channels</div>");

    for(const tag of tagsVocabulary) {

      const count = tagCountMap.get(tag) ?? 0;
      const checkedAttr = (count === totalEnabled) ? " checked" : "";
      const indeterminate = ((count > 0) && (count < totalEnabled)) ? " data-indeterminate=\"true\"" : "";

      lines.push("<label class=\"provider-option\" onclick=\"event.preventDefault(); bulkToggleTag('" + escapeHtml(tag) +
        "', !this.querySelector('input').checked)\">" +
        "<input type=\"checkbox\" class=\"tag-bulk-toggle\" data-tag=\"" + escapeHtml(tag) + "\"" + checkedAttr + indeterminate + "> " +
        escapeHtml(tag) +
        "<span class=\"quick-action-count\" data-tag-count=\"" + escapeHtml(tag) + "\">" +
        String(count) + " of " + String(totalEnabled) + "</span></label>");
    }

    lines.push("</div>");
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

  // Service pills. Clicking a service auto-fills the URL field, which triggers the existing URL-change infrastructure (datalist population, profile
  // resolution). The pills use serviceDisplaySpan for icon rendering via the shared processServiceDisplays path. Guide-grid services fill with their
  // guideUrl; non-guide-grid services fill with https://{domain}/.
  const addFormTags = getAllServiceTags();
  const addFormGuideUrls = getProviderGuideUrls();

  lines.push("<div class=\"form-row\"><label>Service</label>");
  lines.push("<div class=\"provider-pills\">");

  for(const tagInfo of addFormTags) {

    if(tagInfo.tag === "direct") {

      continue;
    }

    const pillUrl = addFormGuideUrls[tagInfo.tag] ?? (tagInfo.domain ? "https://" + tagInfo.domain + "/" : "");

    lines.push("<button type=\"button\" class=\"provider-pill\" data-slug=\"" + escapeHtml(tagInfo.tag) + "\" data-url=\"" + escapeHtml(pillUrl) +
      "\" onclick=\"selectServicePill(this)\">" + serviceDisplaySpan(tagInfo.displayName, tagInfo.domain, tagInfo.iconUrl) + "</button>");
  }

  lines.push("</div>");
  lines.push("</div>");
  lines.push("<div class=\"hint\">Select a service to auto-fill the URL, or enter one manually below.</div>");

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

  // Inline hint for predefined channel matches. Hidden by default, shown by the URL field change listener in config.ts when the entered URL's domain matches a
  // predefined channel. This extends the form's existing URL-based intelligence (channelSelector suggestions, stationId auto-fill, profile auto-detection).
  lines.push("<div id=\"add-predefined-hint\" class=\"hint predefined-hint\" style=\"display: none;\"></div>");

  // Profile dropdown.
  lines.push(...generateProfileDropdown("add-profile", formValues?.get("profile") ?? "", profiles));

  // Advanced fields (station ID, channel selector, channel number, HDHR).
  lines.push(...generateAdvancedFields("add", {

    channelNumberValue: formValues?.get("channelNumber") ?? "",
    channelSelectorValue: formValues?.get("channelSelector") ?? "",
    hdhrEnabled: formValues?.get("hdhrEnabled") !== "false",
    stationIdValue: formValues?.get("stationId") ?? ""
  }));

  // Form buttons.
  lines.push("<div class=\"form-buttons\">");
  lines.push("<button type=\"submit\" class=\"btn btn-primary\">Add Channel</button>");
  lines.push("<button type=\"button\" class=\"btn btn-secondary\" onclick=\"document.getElementById('add-channel-form').style.display='none';\">Cancel</button>");
  lines.push("</div>");

  lines.push("</form>");
  lines.push("</div>"); // End add-channel-form.

  // Profile reference section (hidden by default, toggled via link in profile dropdown hint).
  lines.push(generateProfileReference(profiles));

  // Channels table. Disabled predefined channels are hidden by default and revealed via the "Show disabled" toggle. The wrapper div enables horizontal scrolling on
  // small screens. Table classes dynamically include hide-col-* for each hidden optional column.
  const tableClasses = [ "channel-table", "hide-disabled" ];

  for(const col of OPTIONAL_COLUMNS) {

    if(!visibleCols.has(col.field)) {

      tableClasses.push("hide-" + col.cssClass);
    }
  }

  const sortField = CONFIG.channels.channelSortField;
  const sortDir = CONFIG.channels.channelSortDirection;

  // Service filter toolbar with multi-select dropdown and chips.
  lines.push(generateServiceFilterToolbar());

  // Channel summary line with predefined/user breakdown, placed directly above the table for visual proximity. The user-count span contains the entire user
  // portion (comma, count, and label) so the client can toggle it by setting textContent. When there are no user channels, the span is empty.
  const userPortion = (userCount > 0) ? ", " + String(userCount) + " user" : "";

  lines.push("<div class=\"channel-summary\"><span id=\"total-count\">" + String(totalCount) + "</span> channels " +
    "(<span id=\"predefined-count\">" + String(predefinedCount) + "</span> predefined<span id=\"user-count\">" + userPortion + "</span>) &middot; " +
    "<span id=\"enabled-count\">" + String(enabledCount) + "</span> enabled &middot; " +
    "<span id=\"disabled-count\">" + String(totalHiddenCount) + "</span> disabled</div>");

  // Channels table. The wrapper uses fit-content so it shrinks when columns are hidden, with max-width: 100% to prevent viewport overflow. Centered via auto
  // margins. Dropdown menus escape the container via portal to <body>.
  lines.push("<div class=\"channel-table-wrapper\">");
  lines.push("<table class=\"" + tableClasses.join(" ") + "\" data-sort-field=\"" + sortField + "\" data-sort-dir=\"" + sortDir + "\">");
  lines.push("<thead>");
  lines.push("<tr>");

  // Sortable column headers. All columns except Actions are sortable. The active sort column gets a direction indicator triangle.
  const sortableHeaders: { cssClass: string; field: string; label: string }[] = [

    { cssClass: "col-key", field: "key", label: "Key" },
    { cssClass: "col-name", field: "name", label: "Name" },
    { cssClass: "col-provider", field: "provider", label: "Service" },
    ...OPTIONAL_COLUMNS
  ];

  // All sortable headers use the same DOM structure: <th onclick> wraps a <span class="sort-label"> for the label text. The sort update logic targets
  // .sort-label to modify only the label — never touching other children like the Tags filter dropdown. Clicking anywhere on the <th> triggers sort;
  // additional children (like the filter button) use event.stopPropagation() to prevent sort when interacting with them.
  for(const hdr of sortableHeaders) {

    const isActive = (sortField === hdr.field);
    const activeIndicator = isActive ? ((sortDir === "asc") ? " &#9650;" : " &#9660;") : "";

    lines.push("<th class=\"" + hdr.cssClass + " sortable\" data-sort-field=\"" + hdr.field +
      "\" onclick=\"sortChannelTable('" + hdr.field + "')\">");
    lines.push("<span class=\"sort-label\">" + hdr.label + activeIndicator + "</span>");

    // Tags header: additional filter dropdown alongside the sort label. The dropdown is a client-side-only view filter (transient, not persisted) that
    // shows/hides rows based on their data-channel-tags attribute. The dropdown content comes from generateTagFilterContent() — the single source of truth
    // shared with the tag CRUD incremental update path.
    if(hdr.field === "tags") {

      lines.push("<div class=\"dropdown tag-filter-dropdown\" style=\"display: inline;\">");
      lines.push("<button type=\"button\" class=\"btn-icon btn-tag-filter\" title=\"Filter by tag\" " +
        "onclick=\"event.stopPropagation(); toggleDropdown(this)\">" + ICON_FILTER + "</button>");
      lines.push("<div class=\"dropdown-menu\" id=\"tag-filter-menu\">" + generateTagFilterContent() + "</div>");
      lines.push("</div>");

      // Playlist hint icon. Hidden by default, shown by applyTagColumnFilter() when the filter is active. Clicking opens a popover with the playlist URL
      // that corresponds to the current tag filter, so users can copy it for Channels DVR configuration.
      lines.push("<div class=\"dropdown\" style=\"display: inline;\">");
      lines.push("<button type=\"button\" class=\"btn-icon btn-playlist-hint\" id=\"playlist-hint-btn\" " +
        "title=\"Playlist URL for this filter\" style=\"display: none;\" " +
        "onclick=\"event.stopPropagation(); showPlaylistHint(this)\">" + ICON_LINK + "</button>");
      lines.push("<div class=\"dropdown-menu playlist-hint-menu\"></div>");
      lines.push("</div>");
    }

    lines.push("</th>");
  }

  // Actions header with table options dropdown (kebab menu). Contains the show-disabled toggle and column visibility checkboxes.
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

  // Browse Channels modal. The shell is server-rendered using wizard modal CSS classes. The content area (service picker, channel list) is rendered
  // client-side after fetching discovered channels from the services endpoint.
  lines.push(generateBrowseModal());

  // Service Setup wizard modal. Follows the same wizard pattern as the service profile builder. Three steps: pick services, authenticate, browse channels.
  // The setupCompleted flag is embedded as a data attribute so the client can auto-show the wizard on first visit.
  lines.push(generateSetupWizardModal());

  // Tag Management modal. A simple dialog for creating, deleting, and restoring organizational tags. Client-side handlers drive the CRUD operations via the
  // tag management API endpoints.
  lines.push(generateTagManagementModal());

  // Inline tag edit portal — a shared dropdown portaled to <body> on first use by getTagPortal() in config.ts. One instance shared across all channel rows;
  // populated dynamically from the clicked cell's data-tags attribute. Positioned via getBoundingClientRect() to escape the table wrapper's overflow:auto clipping.
  const vocabulary = getActiveTagVocabulary();

  lines.push("<div id=\"inline-tag-portal\" class=\"dropdown-menu inline-tag-menu\">");

  for(const tag of vocabulary) {

    lines.push("<label class=\"provider-option\" onclick=\"event.stopPropagation()\">" +
      "<input type=\"checkbox\" class=\"inline-tag-checkbox\" data-tag=\"" + escapeHtml(tag) + "\"> " + escapeHtml(tag) + "</label>");
  }

  lines.push("</div>");

  return lines.join("\n");
}
