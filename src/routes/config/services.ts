/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * services.ts: Service profile UI and route handlers for the PrismCast configuration interface.
 */
import type { DomainConfig, SiteProfile } from "../../types/index.ts";
import type { Express, Request, Response } from "express";
import { ICON_ADD, ICON_DELETE, ICON_EDIT, ICON_EXPORT, ICON_IMPORT } from "../icons.ts";
import { LOG, escapeHtml, sanitizeString, stringifySorted } from "../../utils/index.ts";
import { deleteUserProfile, getUserDomains, getUserProfiles, mutateProfiles, validateDomain, validateProfile,
  validateProfileKey } from "../../config/userProfiles.ts";
import { endLoginMode, getLoginPage, startLoginMode } from "../../browser/index.ts";
import { exportServicePack, importServicePack, parseServicePack } from "../../config/servicePacks.ts";
import { getBuiltinProfile, getProfiles } from "../../config/profiles.ts";
import { getChannelListing, validateChannelUrl } from "../../config/userChannels.ts";
import { sendErrorResponse, sendNotFoundError, sendSuccess, sendValidationError } from "./http/envelope.ts";
import { ACTIONS } from "../clientActions.ts";
import type { ProfileInfo } from "../../config/profiles.ts";
import { categorizeProfiles } from "./index.ts";
import { generateWizardModal } from "../components.ts";

/**
 * One field the profile wizard renders and round-trips. The same descriptor shape types both the per-strategy field lists (WIZARD_STRATEGIES) and the flat
 * profile-field registry (WIZARD_FIELDS), so the client renders either from one data-driven convention: a "boolean" field is a checkbox described by its description,
 * a "text" field is an input described by its hint. placeholder is a text-arm convention (a boolean field never carries one). required is honored only by the
 * strategy fields' step-2 validation.
 */
interface WizardFieldDescriptor {

  // Longer explanatory text shown beneath a boolean field's checkbox.
  readonly description?: string;

  // Short hint shown beneath a text field's input.
  readonly hint?: string;

  // The profile (or channelSelection sub-field) key this descriptor reads and writes.
  readonly id: string;

  // The field's visible label.
  readonly label: string;

  // Placeholder text for a text field's input.
  readonly placeholder?: string;

  // Whether step-2 validation requires a strategy text field to be non-empty.
  readonly required?: boolean;

  // Whether the field is a checkbox ("boolean") or a text input ("text").
  readonly type: "boolean" | "text";
}

/**
 * One user-configurable channel-selection strategy in the wizard: its id, display name, description, and the fields it exposes.
 */
interface WizardStrategy {

  // Short description of when to use the strategy.
  readonly description: string;

  // The strategy's configurable fields, rendered in step 2.
  readonly fields: readonly WizardFieldDescriptor[];

  // The strategy identifier persisted as channelSelection.strategy.
  readonly id: string;

  // The strategy's display name.
  readonly name: string;
}

/**
 * Counts channels per user profile key by scanning the channel listing. Returns a record mapping profile key to channel count.
 * @param profileKeys - The set of user profile keys to count channels for.
 * @returns Record of profile key to channel count.
 */
function countChannelsByProfile(profileKeys: Set<string>): Record<string, number> {

  const counts: Record<string, number> = {};
  const listing = getChannelListing();

  for(const entry of listing) {

    const prof = entry.channel.profile;

    if(prof && profileKeys.has(prof)) {

      counts[prof] = (counts[prof] ?? 0) + 1;
    }
  }

  return counts;
}

/**
 * Generates the Custom Profiles panel content for the Channels tab's Custom Profiles subtab. Shows a table of user-defined profiles with their domain mappings and
 * edit/delete buttons. The toolbar provides New Profile, Import, and Export actions. The profile builder wizard is triggered from the toolbar.
 * @returns HTML content for the Custom Profiles panel.
 */
export function generateCustomProfilesPanel(): string {

  const userProfiles = getUserProfiles();
  const userDomains = getUserDomains();
  const lines: string[] = [];

  // Panel description.
  lines.push("<div class=\"settings-panel-description\">");
  lines.push("<p>Define custom site profiles and domain mappings to add support for streaming services not built into PrismCast.</p>");
  lines.push("<p class=\"description-hint\">Tip: Use the <strong>Profile Builder</strong> wizard to create new profiles step by step, ");
  lines.push("or <strong>Import</strong> a pre-made service pack shared by others. User profiles extend builtin profiles and can override ");
  lines.push("specific behaviors like channel selection strategy or playback controls.</p>");
  lines.push("</div>");

  // Toolbar with service operations. Icons imported from the shared icon module.
  lines.push("<div class=\"channel-toolbar\">");
  lines.push("<div class=\"toolbar-group\">");
  lines.push("<button type=\"button\" class=\"btn btn-primary btn-sm toolbar-icon-btn\" data-click-action=\"" + ACTIONS.openWizard +
    "\">" + ICON_ADD + " New Profile</button>");
  lines.push("<button type=\"button\" class=\"btn btn-secondary btn-sm toolbar-icon-btn\" data-click-action=\"" + ACTIONS.startServiceImport + "\">" + ICON_IMPORT +
    " Import</button>");

  // Only show export when there are user profiles to export.
  if(Object.keys(userProfiles).length > 0) {

    lines.push("<button type=\"button\" class=\"btn btn-secondary btn-sm toolbar-icon-btn\" data-click-action=\"" + ACTIONS.startServiceExport + "\">" + ICON_EXPORT +
      " Export</button>");
  }

  lines.push("</div>");
  lines.push("</div>");

  const profileKeys = Object.keys(userProfiles).sort();

  // Import preview modal: shows pack contents with optional skip-channels toggle. Always rendered because import is available even when no profiles exist.
  // The content area is filled by client-side JavaScript when the user selects a file.
  lines.push(generateWizardModal({

    buttons: [
      { action: ACTIONS.closeImportModal, label: "Cancel", position: "right" },
      { action: ACTIONS.executeImport, id: "import-btn", label: "Import", position: "right", size: "sm", variant: "primary" }
    ],
    closeAction: ACTIONS.closeImportModal,
    contentId: "import-modal-body",
    id: "import-modal",
    maxWidth: "480px",
    title: "Import Service Pack"
  }));

  // Empty state when no user services are installed.
  if(profileKeys.length === 0) {

    lines.push("<div class=\"empty-state\">");
    lines.push("<p class=\"empty-state-title\">No custom services installed</p>");
    lines.push("<p class=\"empty-state-text\">Custom services let you add support for streaming sites not built into PrismCast. ");
    lines.push("Click <strong>New Profile</strong> to create one using the step-by-step wizard, or <strong>Import</strong> a service ");
    lines.push("pack shared by another user.</p>");
    lines.push("</div>");

    return lines.join("\n");
  }

  // Build a reverse lookup: profile key -> list of domains mapped to it.
  const profileDomains: Record<string, { domain: string; service?: string; serviceTag?: string }[]> = {};

  for(const key of profileKeys) {

    profileDomains[key] = [];
  }

  for(const [ domain, config ] of Object.entries(userDomains)) {

    if(config.profile && (config.profile in userProfiles)) {

      (profileDomains[config.profile] ??= []).push({ domain, service: config.service, serviceTag: config.serviceTag });
    }
  }

  // Compute channel counts per profile key.
  const channelCounts = countChannelsByProfile(new Set(profileKeys));

  // Service table.
  lines.push("<table class=\"channel-table\">");
  lines.push("<thead><tr>");
  lines.push("<th>Profile</th>");
  lines.push("<th>Service</th>");
  lines.push("<th>Base</th>");
  lines.push("<th>Domains</th>");
  lines.push("<th>Strategy</th>");
  lines.push("<th>Channels</th>");
  lines.push("<th class=\"actions-col\">Actions</th>");
  lines.push("</tr></thead>");
  lines.push("<tbody>");

  for(const key of profileKeys) {

    const profile = userProfiles[key];

    if(!profile) {

      continue;
    }

    const domains = profileDomains[key] ?? [];
    const strategy = profile.channelSelection?.strategy ?? "inherited";
    const count = channelCounts[key] ?? 0;

    // Service name: use the first non-empty service name from domain mappings, or a placeholder if none.
    const serviceName = domains.find((d) => d.service)?.service;
    const serviceHtml = serviceName ? escapeHtml(serviceName) : "<span class=\"text-muted\">\u2014</span>";

    // Domain list: show each mapped domain, or a placeholder if none.
    const domainHtml = (domains.length > 0) ?
      domains.map((d) => escapeHtml(d.domain)).join("<br>") :
      "<span class=\"text-muted\">none</span>";

    lines.push("<tr>");
    lines.push("<td><strong>" + escapeHtml(key) + "</strong></td>");
    lines.push("<td>" + serviceHtml + "</td>");
    lines.push("<td>" + escapeHtml(profile.extends ?? "\u2014") + "</td>");
    lines.push("<td>" + domainHtml + "</td>");
    lines.push("<td>" + escapeHtml(strategy) + "</td>");
    lines.push("<td>" + String(count) + "</td>");
    lines.push("<td class=\"actions-col\">");
    lines.push("<div class=\"btn-group\">");
    lines.push("<button type=\"button\" class=\"btn-icon btn-icon-edit\" title=\"Edit\" aria-label=\"Edit\" " +
      "data-click-action=\"" + ACTIONS.editUserProfile + "\" data-profile-key=\"" + escapeHtml(key) + "\">" + ICON_EDIT + "</button>");
    lines.push("<button type=\"button\" class=\"btn-icon btn-icon-delete\" title=\"Delete\" aria-label=\"Delete\" " +
      "data-click-action=\"" + ACTIONS.deleteUserProfile + "\" data-profile-key=\"" + escapeHtml(key) + "\">" + ICON_DELETE + "</button>");
    lines.push("</div>");
    lines.push("</td>");
    lines.push("</tr>");
  }

  lines.push("</tbody>");
  lines.push("</table>");

  // Export modal: profile checklist with select-all and include-channels toggle. The select-all row is hidden by client-side JavaScript when only one profile
  // exists since it would be redundant with the single profile checkbox. Body content is pre-rendered server-side.
  const exportBody = [
    "<div id=\"export-select-all-row\" class=\"export-section-header\">",
    "<label class=\"export-option-label\">",
    "<input type=\"checkbox\" id=\"export-select-all\" checked data-change-action=\"" + ACTIONS.toggleExportAll + "\"> Select all</label>",
    "</div>",
    "<div id=\"export-profile-list\"></div>",
    "<div class=\"export-divider\"></div>",
    "<label class=\"export-option-label\">",
    "<input type=\"checkbox\" id=\"export-include-channels\"> Include channels</label>",
    "<div class=\"export-hint\">Bundle the channel definitions assigned to these profiles so recipients can import a complete lineup.</div>"
  ].join("\n");

  lines.push(generateWizardModal({

    body: exportBody,
    buttons: [
      { action: ACTIONS.closeExportModal, label: "Cancel", position: "right" },
      { action: ACTIONS.executeExport, id: "export-btn", label: "Export", position: "right", size: "sm", variant: "primary" }
    ],
    closeAction: ACTIONS.closeExportModal,
    id: "export-modal",
    maxWidth: "480px",
    title: "Export Service Profiles"
  }));

  return lines.join("\n");
}

/**
 * Generates the profile builder's multi-step wizard modal. Configures the shared wizard modal shell with profile-specific steps (Base, Strategy, Flags, Domain,
 * Save), navigation buttons, and embedded JSON data registries (profiles, strategies, flags) for the client-side wizard controller.
 * @returns HTML string for the wizard modal.
 */
export function generateProfileWizardModal(): string {

  // Build the profile data for Step 1 radio buttons. getProfiles() returns both builtin and user-defined profiles; we narrow the set to builtin only below.
  const profiles = getProfiles();
  const groups = categorizeProfiles(profiles);

  // Only general builtin profiles are shown in the wizard. Service profiles live in a separate table (PROVIDER_PROFILES) and are already excluded from
  // getProfiles(). User-defined profiles are also excluded because chained extensions (custom B extends custom A extends builtin X) are not supported.
  const include = (p: ProfileInfo): boolean => (p.source === "builtin");

  // Serialize profile groups as JSON for the wizard JavaScript. Each entry has name, description, and summary.
  const profileData = {

    api: groups.api.filter(include).map((p) => ({ description: p.description, name: p.name, summary: p.summary })),
    custom: groups.custom.filter(include).map((p) => ({ description: p.description, name: p.name, summary: p.summary })),
    keyboard: groups.keyboard.filter(include).map((p) => ({ description: p.description, name: p.name, summary: p.summary })),
    multiChannel: groups.multiChannel.filter(include).map((p) => ({ description: p.description, name: p.name, summary: p.summary })),
    special: groups.special.filter(include).map((p) => ({ description: p.description, name: p.name, summary: p.summary }))
  };

  // Server-side registries for the wizard. Strategies define user-configurable channel selection approaches (service-specific strategies like foxGrid, slingGrid,
  // etc. are builtin only and never appear in the wizard). WIZARD_FIELDS defines the profile-level fields exposed in step 3 - boolean flags plus text selectors -
  // rendered by type. Both are serialized as JSON so the wizard client code is fully data-driven: adding a new strategy field or profile field only requires
  // updating these arrays. Every WIZARD_FIELDS id must be a valid SiteProfile field so the save round-trips through validateProfile.
  const WIZARD_STRATEGIES: readonly WizardStrategy[] = [
    {

      description: "Click a channel tile, optionally click a play button.",
      fields: [
        { hint: "Finds the channel element. Use {channel} as the placeholder for the channel selector value.", id: "matchSelector",
          label: "Match Selector (CSS)", placeholder: "e.g., [style*=\"{channel}\"]", required: true, type: "text" },
        { hint: "Play button that appears after clicking the tile. Leave empty if the site auto-plays.", id: "playSelector",
          label: "Play Selector (CSS)", placeholder: "e.g., [aria-label^=\"on now,\"]", required: false, type: "text" },
        { hint: "Scroll the page to the bottom before looking for channel tiles. Useful for lazy-loaded content.", id: "scrollToBottom",
          label: "Scroll to bottom before selection", required: false, type: "boolean" }
      ],
      id: "tileClick",
      name: "Tile Click"
    },
    {

      description: "Match a channel thumbnail image, click adjacent entry.",
      fields: [
        { hint: "Finds the channel element. Use {channel} as the placeholder for the channel selector value.", id: "matchSelector",
          label: "Match Selector (CSS)", placeholder: "e.g., img[src*=\"{channel}\"]", required: true, type: "text" },
        { hint: "Play button that appears after clicking the thumbnail row. Leave empty if the site auto-plays.", id: "playSelector",
          label: "Play Selector (CSS)", placeholder: "", required: false, type: "text" },
        { hint: "Scroll the page to the bottom before looking for channel thumbnails. Useful for lazy-loaded content.", id: "scrollToBottom",
          label: "Scroll to bottom before selection", required: false, type: "boolean" }
      ],
      id: "thumbnailRow",
      name: "Thumbnail Row"
    },
    {

      description: "No channel selection needed.",
      fields: [],
      id: "none",
      name: "None (single-channel site)"
    }
  ];

  const WIZARD_FIELDS: readonly WizardFieldDescriptor[] = [
    { description: "Find video with readyState >= 3. For pages with multiple video elements.", id: "selectReadyVideo", label: "Select ready video", type: "boolean" },
    { description: "Prevent the site from auto-muting.", id: "lockVolumeProperties", label: "Lock volume properties", type: "boolean" },
    { description: "Click an element to start playback.", id: "clickToPlay", label: "Click to play", type: "boolean" },
    { description: "Video is embedded in an iframe.", id: "needsIframeHandling", label: "Needs iframe handling", type: "boolean" },
    { description: "Wait for network to settle before capture.", id: "waitForNetworkIdle", label: "Wait for network idle", type: "boolean" },
    { description: "Force JavaScript requestFullscreen() API.", id: "useRequestFullscreen", label: "Use request fullscreen", type: "boolean" },
    { hint: "Overlay elements to hide during capture.", id: "hideSelector", label: "Hide Selector (CSS)", placeholder: "e.g., .overlay, .ad-banner", type: "text" },
    { hint: "A CSS selector for a site modal to dismiss automatically while tuning. Save & Test checks it, and a malformed selector is disabled at runtime with a " +
      "logged warning.", id: "dismissSelector", label: "Dismiss Selector (CSS)", placeholder: "e.g., .watch-live-modal", type: "text" }
  ];

  return generateWizardModal({

    buttons: [
      { id: "wizard-back", label: "Back", position: "left", role: "back", visible: false },
      { label: "Cancel", position: "right", role: "close" },
      { id: "wizard-next", label: "Next", position: "right", role: "next", variant: "primary" },
      { action: ACTIONS.saveProfile, data: { withTest: "false" }, id: "wizard-save", label: "Save", position: "right", variant: "primary", visible: false },
      { action: ACTIONS.saveProfile, data: { withTest: "true" }, id: "wizard-save-test", label: "Save & Test", position: "right",
        variant: "primary", visible: false }
    ],
    contentId: "wizard-content",
    dataBlocks: [
      "<script>window.__wizardProfiles = " + JSON.stringify(profileData) + ";window.__wizardStrategies = " + JSON.stringify(WIZARD_STRATEGIES) +
        ";window.__wizardFields = " + JSON.stringify(WIZARD_FIELDS) + ";</script>"
    ],
    errorId: "wizard-error",
    id: "wizard-modal",
    steps: [ "Base", "Strategy", "Flags", "Domain", "Save" ],
    title: "New Service Profile",
    titleId: "wizard-title"
  });
}

/**
 * Installs all profile-related route handlers on the Express application.
 * @param app - The Express application.
 */
export function setupProfileRoutes(app: Express): void {

  // GET /config/profiles - List user-defined profiles and domain mappings.
  app.get("/config/profiles", (_req: Request, res: Response): void => {

    try {

      const profiles = getUserProfiles();
      const domains = getUserDomains();

      // Compute channel counts per profile key.
      const profileChannelCounts = countChannelsByProfile(new Set(Object.keys(profiles)));

      // Build a summary for each profile including its domain mappings and channel count.
      const profileList = Object.entries(profiles).toSorted(([a], [b]) => a.localeCompare(b)).map(([ key, profile ]) => {

        // Find domains that reference this profile. The `config.profile === key` join is the projection's single source of truth - the client never re-implements
        // it. Each row carries the full raw DomainConfig so the wizard can round-trip every domain-level field it does not render (videoTimeout, loginUrl,
        // maxContinuousPlayback, dismissSelector) rather than dropping them on an edit-resave; service and serviceTag are surfaced flat for the rendered inputs.
        const profileDomains = Object.entries(domains).filter(([ , config ]) => (config.profile === key)).map(([ domain, config ]) => ({

          config,
          domain,
          service: config.service ?? "",
          serviceTag: config.serviceTag ?? ""
        }));

        return {

          channelCount: profileChannelCounts[key] ?? 0,
          domains: profileDomains,
          extends: profile.extends ?? "default",
          key,
          profile,
          strategy: profile.channelSelection?.strategy ?? "inherited"
        };
      });

      sendSuccess(res, { data: { domains, profiles: profileList } });
    } catch(error) {

      sendErrorResponse(res, error, "list profiles");
    }
  });

  // POST /config/profiles - Create or update a user-defined profile with domain mappings.
  app.post("/config/profiles", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as {
        domains?: Record<string, DomainConfig>;
        key?: string;
        profile?: SiteProfile;
      };

      const key = sanitizeString(body.key ?? "");
      const profile = body.profile;
      const domainMappings = body.domains;

      // Validate key.
      if(!key) {

        sendValidationError(res, "Profile key is required.");

        return;
      }

      // Validate profile object.
      if(!profile || (typeof profile !== "object")) {

        sendValidationError(res, "Profile definition is required.");

        return;
      }

      // Sanitize string fields in the profile object, including nested channelSelection strings.
      for(const [ field, value ] of Object.entries(profile)) {

        if(typeof value === "string") {

          (profile as Record<string, unknown>)[field] = sanitizeString(value);
        }
      }

      if(profile.channelSelection?.matchSelector) {

        profile.channelSelection.matchSelector = sanitizeString(profile.channelSelection.matchSelector);
      }

      // Sanitize string fields in domain mappings if provided.
      if(domainMappings) {

        for(const [ domain, config ] of Object.entries(domainMappings)) {

          for(const [ field, value ] of Object.entries(config)) {

            if(typeof value === "string") {

              (config as Record<string, unknown>)[field] = sanitizeString(value);
            }
          }

          // Sanitize domain keys themselves. If the sanitized domain differs from the original, replace the entry.
          const cleanDomain = sanitizeString(domain);

          if(cleanDomain !== domain) {

            domainMappings[cleanDomain] = config;
            Reflect.deleteProperty(domainMappings, domain);
          }
        }
      }

      // Check if this is a new profile or an update to an existing one.
      const existingProfiles = getUserProfiles();
      const isNew = !(key in existingProfiles);

      // Validate the key format and builtin conflict.
      const keyError = validateProfileKey(key, isNew);

      if(keyError) {

        sendValidationError(res, keyError);

        return;
      }

      // Validate the profile content: extends must reference a builtin profile, strategy must be recognized and generic, all flags must be valid SiteProfile fields.
      const profileErrors = validateProfile(key, profile);

      if(profileErrors.length > 0) {

        sendValidationError(res, profileErrors.join(" "));

        return;
      }

      // Validate domain mappings if provided. A mapping may reference any builtin profile, any profile already in the user store, or the profile being saved.
      // Profile existence questions go through the single builtin lookup rather than an enumerated table, which is what lets a mapping name a provider profile
      // or a provider module's registered profile - neither appears in the UI profile catalog.
      if(domainMappings && (Object.keys(domainMappings).length > 0)) {

        const isKnownProfile = (name: string): boolean => Boolean(getBuiltinProfile(name)) || (name in existingProfiles) || (name === key);
        const domainErrors: string[] = [];

        for(const [ domain, config ] of Object.entries(domainMappings)) {

          domainErrors.push(...validateDomain(domain, config, isKnownProfile));
        }

        if(domainErrors.length > 0) {

          sendValidationError(res, domainErrors.join(" "));

          return;
        }
      }

      // Save the profile and domain mappings. The read-modify-write happens inside the mutator callback so the merge applies against the latest serialized
      // state under the per-store queue's lock - concurrent POSTs to different keys each see the other's writes and both land on disk. When updating an existing
      // profile, stale domain mappings that pointed to it are stripped first so the new mappings replace them cleanly rather than coexisting.
      await mutateProfiles((data) => {

        for(const [ domain, config ] of Object.entries(data.domains)) {

          if(config.profile === key) {

            Reflect.deleteProperty(data.domains, domain);
          }
        }

        data.profiles[key] = profile;

        if(domainMappings) {

          Object.assign(data.domains, domainMappings);
        }
      });

      const actionLabel = isNew ? "created" : "updated";

      LOG.info("User profile '%s' %s.", key, actionLabel);

      sendSuccess(res, {

        data: { key, panelHtml: generateCustomProfilesPanel() },
        message: "Profile '" + key + "' " + actionLabel + " successfully."
      });
    } catch(error) {

      sendErrorResponse(res, error, "save profile");
    }
  });

  // DELETE /config/profiles/:key - Delete a user-defined profile and its domain mappings.
  app.delete("/config/profiles/:key", async (req: Request, res: Response): Promise<void> => {

    try {

      const key = req.params["key"] as string;

      if(!key) {

        sendValidationError(res, "Profile key is required.");

        return;
      }

      // Verify the profile exists as a user profile.
      const userProfiles = getUserProfiles();

      if(!(key in userProfiles)) {

        sendNotFoundError(res, "Profile '" + key + "' not found.");

        return;
      }

      await deleteUserProfile(key);

      sendSuccess(res, {

        data: { key, panelHtml: generateCustomProfilesPanel() },
        message: "Profile '" + key + "' deleted successfully."
      });
    } catch(error) {

      sendErrorResponse(res, error, "delete profile");
    }
  });

  // POST /config/profiles/import - Import a service pack. Accepts optional skipChannels flag in the request body.
  app.post("/config/profiles/import", async (req: Request, res: Response): Promise<void> => {

    try {

      const rawData = req.body as Record<string, unknown>;
      const skipChannels = rawData["skipChannels"] === true;

      // Parse and validate the service pack. The skipChannels flag read above is threaded into both stages of the import, so a pack whose channels are being
      // skipped is not judged on them: the parse leaves them out of the result and the import never looks for them.
      const parseResult = parseServicePack(rawData, { skipChannels });

      if(!parseResult.pack) {

        sendValidationError(res, "Validation errors:\n" + parseResult.errors.join("\n"));

        return;
      }

      // Import the validated pack. Profile/domain save failures are fatal; channel import failures are non-fatal warnings included in the response.
      const importResult = await importServicePack(parseResult.pack, { skipChannels });

      if(!importResult.success) {

        sendValidationError(res, "Import failed:\n" + importResult.errors.join("\n"));

        return;
      }

      const parts: string[] = [];

      if(importResult.profilesAdded > 0) {

        parts.push(String(importResult.profilesAdded) + " profile(s)");
      }

      if(importResult.domainsAdded > 0) {

        parts.push(String(importResult.domainsAdded) + " domain mapping(s)");
      }

      if(importResult.channelsAdded > 0) {

        parts.push(String(importResult.channelsAdded) + " channel(s)");
      }

      const summary = "Imported " + parts.join(", ") + " from '" + parseResult.pack.name + "'.";

      LOG.info("%s", summary);

      // Include any non-fatal warnings (e.g., channel import failures) so the client can report what succeeded and what didn't.
      if(importResult.errors.length > 0) {

        for(const warning of importResult.errors) {

          LOG.warn("Import warning: %s.", warning);
        }
      }

      sendSuccess(res, {

        data: { panelHtml: generateCustomProfilesPanel(), summary: importResult, warnings: importResult.errors },
        message: summary
      });
    } catch(error) {

      sendErrorResponse(res, error, "import service pack");
    }
  });

  // GET /config/profiles/export - Export one or more user profiles as a service pack. Accepts comma-separated profile keys.
  app.get("/config/profiles/export", (req: Request, res: Response): void => {

    try {

      const profileParam = req.query["profile"] as string | undefined;
      const includeDomains = req.query["domains"] !== "0";
      const includeChannels = req.query["channels"] === "1";
      const name = req.query["name"] as string | undefined;

      if(!profileParam) {

        sendValidationError(res, "Profile key is required (use ?profile=key).");

        return;
      }

      // Split comma-separated keys and trim whitespace.
      const profileKeys = profileParam.split(",").map((k) => k.trim()).filter(Boolean);

      if(profileKeys.length === 0) {

        sendValidationError(res, "No valid profile keys provided.");

        return;
      }

      const pack = exportServicePack(profileKeys, { includeChannels, includeDomains, name: name ?? profileKeys.join(", ") });

      if(!pack) {

        sendNotFoundError(res, "None of the requested profiles were found.");

        return;
      }

      // Use the first profile key for the filename when exporting a single profile; fall back to a generic name for multi-profile packs or empty lists.
      const filename = ((profileKeys.length === 1) ? profileKeys[0] : undefined) ?? "prismcast";

      res.setHeader("Content-Type", "application/json");
      res.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "-service-pack.json\"");
      res.send(stringifySorted(pack) + "\n");
    } catch(error) {

      sendErrorResponse(res, error, "export service pack");
    }
  });

  // POST /config/profiles/test - Start a test flow by navigating a visible browser tab to the candidate profile's URL for manual selector
  // verification (no profile behaviors are applied).
  app.post("/config/profiles/test", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { url?: string };
      const url = body.url?.trim();

      if(!url) {

        sendValidationError(res, "URL is required.");

        return;
      }

      // Validate URL format before opening the browser.
      const urlError = validateChannelUrl(url);

      if(urlError) {

        sendValidationError(res, urlError);

        return;
      }

      const result = await startLoginMode(url);

      if(!result.success) {

        sendValidationError(res, result.error ?? "Failed to start test.");

        return;
      }

      sendSuccess(res, { message: "Test page opened. The browser window should be visible." });
    } catch(error) {

      sendErrorResponse(res, error, "start test");
    }
  });

  // POST /config/profiles/test/check - Check CSS selectors against the live test page.
  app.post("/config/profiles/test/check", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { selectors?: Record<string, string> };
      const selectors = body.selectors;

      if(!selectors || (typeof selectors !== "object")) {

        sendValidationError(res, "Selectors object is required.");

        return;
      }

      const page = getLoginPage();

      if(!page) {

        sendValidationError(res, "No active test page. Start a test first.");

        return;
      }

      // Evaluate all selectors in a single page.evaluate call to avoid await-in-loop.
      const counts = await page.evaluate((selectorMap: Record<string, string>) => {

        const output: Record<string, number> = {};

        for(const [ name, sel ] of Object.entries(selectorMap)) {

          try {

            output[name] = document.querySelectorAll(sel).length;
          } catch {

            // A negative sentinel signals an invalid selector back across the evaluate boundary; the caller treats any count below zero as invalid.
            output[name] = -1;
          }
        }

        return output;
      }, selectors);

      const results: Record<string, { count: number; valid: boolean }> = {};

      for(const [ name, count ] of Object.entries(counts)) {

        results[name] = { count: Math.max(count, 0), valid: count >= 0 };
      }

      sendSuccess(res, { data: { results } });
    } catch(error) {

      sendErrorResponse(res, error, "check selectors");
    }
  });

  // POST /config/profiles/test/done - End an active test flow.
  app.post("/config/profiles/test/done", async (_req: Request, res: Response): Promise<void> => {

    try {

      await endLoginMode();

      sendSuccess(res, { data: { panelHtml: generateCustomProfilesPanel() }, message: "Test flow ended." });
    } catch(error) {

      sendErrorResponse(res, error, "end test");
    }
  });
}
