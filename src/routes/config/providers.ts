/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * providers.ts: Provider profile UI and route handlers for the PrismCast configuration interface.
 */
import type { DomainConfig, SiteProfile } from "../../types/index.js";
import { EXCLUDED_PROFILES, deleteUserProfile, getUserDomains, getUserProfiles, saveUserProfiles, validateDomain, validateProfile,
  validateProfileKey } from "../../config/userProfiles.js";
import type { Express, Request, Response } from "express";
import { ICON_DELETE, ICON_EDIT } from "./channels/index.js";
import { LOG, escapeHtml, formatError, sanitizeString } from "../../utils/index.js";
import { exportProviderPack, importProviderPack, parseProviderPack } from "../../config/providerPacks.js";
import { getChannelListing, validateChannelUrl } from "../../config/userChannels.js";
import type { ProfileInfo } from "../../config/profiles.js";
import { categorizeProfiles } from "./index.js";
import { getProfiles } from "../../config/profiles.js";

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
 * Generates the Providers panel content for the Channels tab's Providers subtab. Shows a table of user-defined profiles with their domain mappings and delete
 * buttons. The toolbar provides New Profile, Import, and Export actions. The profile builder wizard is triggered from the toolbar.
 * @returns HTML content for the Providers panel.
 */
export function generateProvidersPanel(): string {

  const userProfiles = getUserProfiles();
  const userDomains = getUserDomains();
  const lines: string[] = [];

  // Panel description.
  lines.push("<div class=\"settings-panel-description\">");
  lines.push("<p>Define custom site profiles and domain mappings to add support for streaming providers not built into PrismCast.</p>");
  lines.push("<p class=\"description-hint\">Tip: Use the <strong>Profile Builder</strong> wizard to create new profiles step by step, ");
  lines.push("or <strong>Import</strong> a pre-made provider pack shared by others. User profiles extend built-in profiles and can override ");
  lines.push("specific behaviors like channel selection strategy or playback controls.</p>");
  lines.push("</div>");

  // Toolbar with provider operations.
  lines.push("<div class=\"channel-toolbar\">");
  lines.push("<div class=\"toolbar-group\">");
  lines.push("<button type=\"button\" class=\"btn btn-primary btn-sm\" onclick=\"openWizard()\">New Profile</button>");
  lines.push("<button type=\"button\" class=\"btn btn-secondary btn-sm\" onclick=\"startProviderImport()\">Import</button>");

  // Only show export when there are user profiles to export.
  if(Object.keys(userProfiles).length > 0) {

    lines.push("<button type=\"button\" class=\"btn btn-secondary btn-sm\" onclick=\"startProviderExport()\">Export</button>");
  }

  lines.push("</div>");
  lines.push("</div>");

  const profileKeys = Object.keys(userProfiles).sort();

  // Import preview modal: shows pack contents with optional skip-channels toggle. Always rendered because import is available even when no profiles exist.
  lines.push("<div id=\"import-modal\" class=\"wizard-modal\" style=\"display: none;\">");
  lines.push("<div class=\"wizard-modal-content\" style=\"max-width: 480px;\">");
  lines.push("<div class=\"wizard-header\">");
  lines.push("<h3>Import Provider Pack</h3>");
  lines.push("<button type=\"button\" class=\"wizard-close\" aria-label=\"Close\" onclick=\"closeImportModal()\">\u2715</button>");
  lines.push("</div>");
  lines.push("<div id=\"import-modal-body\" class=\"export-modal-body\"></div>");
  lines.push("<div class=\"wizard-buttons\">");
  lines.push("<button type=\"button\" class=\"btn btn-secondary\" onclick=\"closeImportModal()\">Cancel</button>");
  lines.push("<div class=\"wizard-buttons-right\">");
  lines.push("<button type=\"button\" id=\"import-btn\" class=\"btn btn-primary btn-sm\" onclick=\"executeImport()\">Import</button>");
  lines.push("</div>");
  lines.push("</div>");
  lines.push("</div>");
  lines.push("</div>");

  // Empty state when no user providers are installed.
  if(profileKeys.length === 0) {

    lines.push("<div class=\"empty-state\">");
    lines.push("<p class=\"empty-state-title\">No custom providers installed</p>");
    lines.push("<p class=\"empty-state-text\">Custom providers let you add support for streaming sites not built into PrismCast. ");
    lines.push("Click <strong>New Profile</strong> to create one using the step-by-step wizard, or <strong>Import</strong> a provider ");
    lines.push("pack shared by another user.</p>");
    lines.push("</div>");

    return lines.join("\n");
  }

  // Build a reverse lookup: profile key → list of domains mapped to it.
  const profileDomains: Record<string, { domain: string; provider?: string; providerTag?: string }[]> = {};

  for(const key of profileKeys) {

    profileDomains[key] = [];
  }

  for(const [ domain, config ] of Object.entries(userDomains)) {

    if(config.profile && (config.profile in userProfiles)) {

      profileDomains[config.profile] ??= [];
      profileDomains[config.profile].push({ domain, provider: config.provider, providerTag: config.providerTag });
    }
  }

  // Compute channel counts per profile key.
  const channelCounts = countChannelsByProfile(new Set(profileKeys));

  // Provider table.
  lines.push("<table class=\"channel-table\">");
  lines.push("<thead><tr>");
  lines.push("<th>Profile</th>");
  lines.push("<th>Provider</th>");
  lines.push("<th>Base</th>");
  lines.push("<th>Domains</th>");
  lines.push("<th>Strategy</th>");
  lines.push("<th>Channels</th>");
  lines.push("<th class=\"actions-col\">Actions</th>");
  lines.push("</tr></thead>");
  lines.push("<tbody>");

  for(const key of profileKeys) {

    const profile = userProfiles[key];
    const domains = profileDomains[key] ?? [];
    const strategy = profile.channelSelection?.strategy ?? "inherited";
    const count = channelCounts[key] ?? 0;

    // Provider name: use the first non-empty provider name from domain mappings, or a placeholder if none.
    const providerName = domains.find((d) => d.provider)?.provider;
    const providerHtml = providerName ? escapeHtml(providerName) : "<span class=\"text-muted\">\u2014</span>";

    // Domain list: show each mapped domain, or a placeholder if none.
    const domainHtml = (domains.length > 0) ?
      domains.map((d) => escapeHtml(d.domain)).join("<br>") :
      "<span class=\"text-muted\">none</span>";

    lines.push("<tr>");
    lines.push("<td><strong>" + escapeHtml(key) + "</strong></td>");
    lines.push("<td>" + providerHtml + "</td>");
    lines.push("<td>" + escapeHtml(profile.extends ?? "\u2014") + "</td>");
    lines.push("<td>" + domainHtml + "</td>");
    lines.push("<td>" + escapeHtml(strategy) + "</td>");
    lines.push("<td>" + String(count) + "</td>");
    lines.push("<td class=\"actions-col\">");
    lines.push("<div class=\"btn-group\">");
    lines.push("<button type=\"button\" class=\"btn-icon btn-icon-edit\" title=\"Edit\" aria-label=\"Edit\" " +
      "onclick=\"editUserProfile('" + escapeHtml(key) + "')\">" + ICON_EDIT + "</button>");
    lines.push("<button type=\"button\" class=\"btn-icon btn-icon-delete\" title=\"Delete\" aria-label=\"Delete\" " +
      "onclick=\"deleteUserProfile('" + escapeHtml(key) + "')\">" + ICON_DELETE + "</button>");
    lines.push("</div>");
    lines.push("</td>");
    lines.push("</tr>");
  }

  lines.push("</tbody>");
  lines.push("</table>");

  // Export modal: profile checklist with select-all and include-channels toggle. The select-all row is hidden by client-side JavaScript when only one profile
  // exists since it would be redundant with the single profile checkbox.
  lines.push("<div id=\"export-modal\" class=\"wizard-modal\" style=\"display: none;\">");
  lines.push("<div class=\"wizard-modal-content\" style=\"max-width: 480px;\">");
  lines.push("<div class=\"wizard-header\">");
  lines.push("<h3>Export Provider Profiles</h3>");
  lines.push("<button type=\"button\" class=\"wizard-close\" aria-label=\"Close\" onclick=\"closeExportModal()\">\u2715</button>");
  lines.push("</div>");
  lines.push("<div class=\"export-modal-body\">");
  lines.push("<div id=\"export-select-all-row\" class=\"export-section-header\">");
  lines.push("<label class=\"export-option-label\">");
  lines.push("<input type=\"checkbox\" id=\"export-select-all\" checked onchange=\"toggleExportAll(this)\"> Select all</label>");
  lines.push("</div>");
  lines.push("<div id=\"export-profile-list\"></div>");
  lines.push("<div class=\"export-divider\"></div>");
  lines.push("<label class=\"export-option-label\">");
  lines.push("<input type=\"checkbox\" id=\"export-include-channels\"> Include channels</label>");
  lines.push("<div class=\"export-hint\">Bundle the channel definitions assigned to these profiles so recipients can import a complete lineup.</div>");
  lines.push("</div>");
  lines.push("<div class=\"wizard-buttons\">");
  lines.push("<button type=\"button\" class=\"btn btn-secondary\" onclick=\"closeExportModal()\">Cancel</button>");
  lines.push("<div class=\"wizard-buttons-right\">");
  lines.push("<button type=\"button\" id=\"export-btn\" class=\"btn btn-primary btn-sm\" onclick=\"executeExport()\">Export</button>");
  lines.push("</div>");
  lines.push("</div>");
  lines.push("</div>");
  lines.push("</div>");

  return lines.join("\n");
}

/**
 * Generates the profile builder wizard modal HTML. The modal shell contains a step indicator, a dynamic content area (rendered by client-side JavaScript), and
 * navigation buttons. Profile data for Step 1 is embedded as a JSON script block for the wizard JS to consume.
 * @returns HTML string for the wizard modal.
 */
export function generateWizardModal(): string {

  // Build the profile data for Step 1 radio buttons. Include all profiles (built-in and user-defined).
  const profiles = getProfiles();
  const groups = categorizeProfiles(profiles);

  // Site-specific profiles are excluded from the wizard — they have strategies and selectors tightly coupled to a specific service's DOM structure. User-defined
  // profiles are also excluded because chained extensions (custom B extends custom A extends built-in X) are not supported.
  const include = (p: ProfileInfo): boolean => !EXCLUDED_PROFILES.has(p.name) && (p.source === "builtin");

  // Serialize profile groups as JSON for the wizard JavaScript. Each entry has name, description, and summary.
  const profileData = {

    api: groups.api.filter(include).map((p) => ({ description: p.description, name: p.name, summary: p.summary })),
    custom: groups.custom.filter(include).map((p) => ({ description: p.description, name: p.name, summary: p.summary })),
    keyboard: groups.keyboard.filter(include).map((p) => ({ description: p.description, name: p.name, summary: p.summary })),
    multiChannel: groups.multiChannel.filter(include).map((p) => ({ description: p.description, name: p.name, summary: p.summary })),
    special: groups.special.filter(include).map((p) => ({ description: p.description, name: p.name, summary: p.summary }))
  };

  const lines: string[] = [];

  lines.push("<div id=\"wizard-modal\" class=\"wizard-modal\" style=\"display: none;\">");
  lines.push("<div class=\"wizard-modal-content\">");

  // Header with title and close button. The title is updated by JavaScript to reflect edit vs. new mode.
  lines.push("<div class=\"wizard-header\">");
  lines.push("<h3 id=\"wizard-title\">New Provider Profile</h3>");
  lines.push("<button type=\"button\" class=\"wizard-close\" aria-label=\"Close\" onclick=\"closeWizard()\">\u2715</button>");
  lines.push("</div>");

  // Step indicator.
  lines.push("<div class=\"wizard-steps\">");
  lines.push("<div class=\"wizard-step active\" data-step=\"1\"><span class=\"step-circle\">1</span><span class=\"step-label\">Base</span></div>");
  lines.push("<div class=\"wizard-step-line\"></div>");
  lines.push("<div class=\"wizard-step\" data-step=\"2\"><span class=\"step-circle\">2</span><span class=\"step-label\">Strategy</span></div>");
  lines.push("<div class=\"wizard-step-line\"></div>");
  lines.push("<div class=\"wizard-step\" data-step=\"3\"><span class=\"step-circle\">3</span><span class=\"step-label\">Flags</span></div>");
  lines.push("<div class=\"wizard-step-line\"></div>");
  lines.push("<div class=\"wizard-step\" data-step=\"4\"><span class=\"step-circle\">4</span><span class=\"step-label\">Domain</span></div>");
  lines.push("<div class=\"wizard-step-line\"></div>");
  lines.push("<div class=\"wizard-step\" data-step=\"5\"><span class=\"step-circle\">5</span><span class=\"step-label\">Save</span></div>");
  lines.push("</div>");

  // Dynamic content area — rendered by wizard JavaScript based on current step.
  lines.push("<div id=\"wizard-content\" class=\"wizard-content\"></div>");

  // Validation error area.
  lines.push("<div id=\"wizard-error\" class=\"wizard-error\" style=\"display: none;\"></div>");

  // Navigation buttons.
  lines.push("<div class=\"wizard-buttons\">");
  lines.push("<button type=\"button\" class=\"btn btn-secondary\" id=\"wizard-back\" onclick=\"wizardBack()\" style=\"display: none;\">Back</button>");
  lines.push("<div class=\"wizard-buttons-right\">");
  lines.push("<button type=\"button\" class=\"btn btn-secondary\" onclick=\"closeWizard()\">Cancel</button>");
  lines.push("<button type=\"button\" class=\"btn btn-primary\" id=\"wizard-next\" onclick=\"wizardNext()\">Next</button>");
  lines.push("<button type=\"button\" class=\"btn btn-primary\" id=\"wizard-save\" onclick=\"saveProfile(false)\" style=\"display: none;\">Save</button>");
  lines.push("<button type=\"button\" class=\"btn btn-primary\" id=\"wizard-save-test\" onclick=\"saveProfile(true)\" ",
    "style=\"display: none;\">Save &amp; Test</button>");
  lines.push("</div>");
  lines.push("</div>");

  lines.push("</div>");
  lines.push("</div>");

  // Server-side registries for the wizard. Strategies define user-configurable channel selection approaches (provider-specific strategies like foxGrid, slingGrid,
  // etc. are built-in only and never appear in the wizard). Flags define boolean profile flags exposed in step 3. Both are serialized as JSON so the wizard client
  // code is fully data-driven — adding a new strategy field or flag only requires updating these arrays.
  const WIZARD_STRATEGIES = [
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

  const WIZARD_FLAGS = [
    { description: "Find video with readyState >= 3. For pages with multiple video elements.", id: "selectReadyVideo", label: "Select ready video" },
    { description: "Prevent the site from auto-muting.", id: "lockVolumeProperties", label: "Lock volume properties" },
    { description: "Click an element to start playback.", id: "clickToPlay", label: "Click to play" },
    { description: "Video is embedded in an iframe.", id: "needsIframeHandling", label: "Needs iframe handling" },
    { description: "Wait for network to settle before capture.", id: "waitForNetworkIdle", label: "Wait for network idle" },
    { description: "Force JavaScript requestFullscreen() API.", id: "useRequestFullscreen", label: "Use request fullscreen" }
  ];

  // Embedded profile data and registries for the wizard JavaScript.
  lines.push("<script>window.__wizardProfiles = " + JSON.stringify(profileData) + ";window.__wizardStrategies = " + JSON.stringify(WIZARD_STRATEGIES) +
    ";window.__wizardFlags = " + JSON.stringify(WIZARD_FLAGS) + ";</script>");

  return lines.join("\n");
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
      const profileList = Object.entries(profiles).sort(([a], [b]) => a.localeCompare(b)).map(([ key, profile ]) => {

        // Find domains that reference this profile.
        const profileDomains = Object.entries(domains).filter(([ , config ]) => (config.profile === key)).map(([ domain, config ]) => ({

          domain,
          provider: config.provider ?? "",
          providerTag: config.providerTag ?? ""
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

      res.json({ domains, profiles: profileList, success: true });
    } catch(error) {

      LOG.error("Failed to list profiles: %s.", formatError(error));
      res.status(500).json({ error: "Failed to list profiles: " + formatError(error), success: false });
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

        res.status(400).json({ error: "Profile key is required.", success: false });

        return;
      }

      // Validate profile object.
      if(!profile || (typeof profile !== "object")) {

        res.status(400).json({ error: "Profile definition is required.", success: false });

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

      // Validate the key format and built-in conflict.
      const keyError = validateProfileKey(key, isNew);

      if(keyError) {

        res.status(400).json({ error: keyError, success: false });

        return;
      }

      // Validate the profile content: extends must reference a built-in profile, strategy must be recognized and generic, all flags must be valid SiteProfile fields.
      const profileErrors = validateProfile(key, profile);

      if(profileErrors.length > 0) {

        res.status(400).json({ error: profileErrors.join(" "), success: false });

        return;
      }

      // Validate domain mappings if provided. Build the available profiles set from built-in + existing user profiles + the profile being saved.
      if(domainMappings && (Object.keys(domainMappings).length > 0)) {

        const availableProfiles = new Set(getProfiles().map((p) => p.name));

        availableProfiles.add(key);

        const domainErrors: string[] = [];

        for(const [ domain, config ] of Object.entries(domainMappings)) {

          domainErrors.push(...validateDomain(domain, config, availableProfiles));
        }

        if(domainErrors.length > 0) {

          res.status(400).json({ error: domainErrors.join(" "), success: false });

          return;
        }
      }

      // Save the profile and domain mappings. When updating an existing profile, remove stale domain mappings that pointed to this profile before applying the new
      // ones. This handles the case where a user changes domain mappings on edit — without this cleanup, old domain entries would persist alongside the new ones.
      const existingDomains = getUserDomains();
      const mergedProfiles = { ...existingProfiles, [key]: profile };
      const cleanedDomains: Record<string, DomainConfig> = {};

      for(const [ domain, config ] of Object.entries(existingDomains)) {

        if(config.profile !== key) {

          cleanedDomains[domain] = config;
        }
      }

      const mergedDomains = { ...cleanedDomains, ...(domainMappings ?? {}) };

      await saveUserProfiles(mergedProfiles, mergedDomains);

      const actionLabel = isNew ? "created" : "updated";

      LOG.info("User profile '%s' %s.", key, actionLabel);

      res.json({ key, message: "Profile '" + key + "' " + actionLabel + " successfully.", success: true });
    } catch(error) {

      LOG.error("Failed to save profile: %s.", formatError(error));
      res.status(500).json({ error: "Failed to save profile: " + formatError(error), success: false });
    }
  });

  // DELETE /config/profiles/:key - Delete a user-defined profile and its domain mappings.
  app.delete("/config/profiles/:key", async (req: Request, res: Response): Promise<void> => {

    try {

      const key = req.params.key as string;

      if(!key) {

        res.status(400).json({ error: "Profile key is required.", success: false });

        return;
      }

      // Verify the profile exists as a user profile.
      const userProfiles = getUserProfiles();

      if(!(key in userProfiles)) {

        res.status(404).json({ error: "Profile '" + key + "' not found.", success: false });

        return;
      }

      await deleteUserProfile(key);

      res.json({ key, message: "Profile '" + key + "' deleted successfully.", success: true });
    } catch(error) {

      LOG.error("Failed to delete profile: %s.", formatError(error));
      res.status(500).json({ error: "Failed to delete profile: " + formatError(error), success: false });
    }
  });

  // POST /config/profiles/import - Import a provider pack. Accepts optional skipChannels flag in the request body.
  app.post("/config/profiles/import", async (req: Request, res: Response): Promise<void> => {

    try {

      const rawData = req.body as Record<string, unknown>;
      const skipChannels = rawData.skipChannels === true;

      // Parse and validate the provider pack. The parseProviderPack function ignores unknown keys like skipChannels.
      const parseResult = parseProviderPack(rawData);

      if(!parseResult.pack) {

        res.status(400).json({ error: "Validation errors:\n" + parseResult.errors.join("\n"), success: false });

        return;
      }

      // Import the validated pack. Profile/domain save failures are fatal; channel import failures are non-fatal warnings included in the response.
      const importResult = await importProviderPack(parseResult.pack, { skipChannels });

      if(!importResult.success) {

        res.status(400).json({ error: "Import failed:\n" + importResult.errors.join("\n"), success: false });

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

          LOG.warn("Import warning: %s", warning);
        }
      }

      res.json({ message: summary, success: true, summary: importResult, warnings: importResult.errors });
    } catch(error) {

      LOG.error("Failed to import provider pack: %s.", formatError(error));
      res.status(500).json({ error: "Failed to import provider pack: " + formatError(error), success: false });
    }
  });

  // GET /config/profiles/export - Export one or more user profiles as a provider pack. Accepts comma-separated profile keys.
  app.get("/config/profiles/export", (req: Request, res: Response): void => {

    try {

      const profileParam = req.query.profile as string | undefined;
      const includeDomains = req.query.domains !== "0";
      const includeChannels = req.query.channels === "1";
      const name = req.query.name as string | undefined;

      if(!profileParam) {

        res.status(400).json({ error: "Profile key is required (use ?profile=key).", success: false });

        return;
      }

      // Split comma-separated keys and trim whitespace.
      const profileKeys = profileParam.split(",").map((k) => k.trim()).filter(Boolean);

      if(profileKeys.length === 0) {

        res.status(400).json({ error: "No valid profile keys provided.", success: false });

        return;
      }

      const pack = exportProviderPack(profileKeys, { includeChannels, includeDomains, name: name ?? profileKeys.join(", ") });

      if(!pack) {

        res.status(404).json({ error: "None of the requested profiles were found.", success: false });

        return;
      }

      // Use the first profile key for the filename when exporting multiple profiles.
      const filename = (profileKeys.length === 1) ? profileKeys[0] : "prismcast";

      res.setHeader("Content-Type", "application/json");
      res.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "-provider-pack.json\"");
      res.send(JSON.stringify(pack, null, 2) + "\n");
    } catch(error) {

      LOG.error("Failed to export provider pack: %s.", formatError(error));
      res.status(500).json({ error: "Failed to export provider pack: " + formatError(error), success: false });
    }
  });

  // POST /config/profiles/test - Start a test flow by opening a URL with the user's profile applied.
  app.post("/config/profiles/test", async (req: Request, res: Response): Promise<void> => {

    try {

      const { startLoginMode } = await import("../../browser/index.js");
      const body = req.body as { url?: string };
      const url = body.url?.trim();

      if(!url) {

        res.status(400).json({ error: "URL is required.", success: false });

        return;
      }

      // Validate URL format before opening the browser.
      const urlError = validateChannelUrl(url);

      if(urlError) {

        res.status(400).json({ error: urlError, success: false });

        return;
      }

      const result = await startLoginMode(url);

      if(!result.success) {

        res.status(400).json({ error: result.error ?? "Failed to start test.", success: false });

        return;
      }

      res.json({ message: "Test page opened. The browser window should be visible.", success: true });
    } catch(error) {

      LOG.error("Failed to start test flow: %s.", formatError(error));
      res.status(500).json({ error: "Failed to start test: " + formatError(error), success: false });
    }
  });

  // POST /config/profiles/test/check - Check CSS selectors against the live test page.
  app.post("/config/profiles/test/check", async (req: Request, res: Response): Promise<void> => {

    try {

      const { getLoginPage } = await import("../../browser/index.js");
      const body = req.body as { selectors?: Record<string, string> };
      const selectors = body.selectors;

      if(!selectors || (typeof selectors !== "object")) {

        res.status(400).json({ error: "Selectors object is required.", success: false });

        return;
      }

      const page = getLoginPage();

      if(!page) {

        res.status(400).json({ error: "No active test page. Start a test first.", success: false });

        return;
      }

      // Evaluate all selectors in a single page.evaluate call to avoid await-in-loop.
      const counts = await page.evaluate((selectorMap: Record<string, string>) => {

        const output: Record<string, number> = {};

        for(const [ name, sel ] of Object.entries(selectorMap)) {

          try {

            output[name] = document.querySelectorAll(sel).length;
          } catch {

            output[name] = -1;
          }
        }

        return output;
      }, selectors);

      const results: Record<string, { count: number; valid: boolean }> = {};

      for(const [ name, count ] of Object.entries(counts)) {

        results[name] = { count: Math.max(count, 0), valid: count >= 0 };
      }

      res.json({ results, success: true });
    } catch(error) {

      LOG.error("Failed to check selectors: %s.", formatError(error));
      res.status(500).json({ error: "Failed to check selectors: " + formatError(error), success: false });
    }
  });

  // POST /config/profiles/test/done - End an active test flow.
  app.post("/config/profiles/test/done", async (_req: Request, res: Response): Promise<void> => {

    try {

      const { endLoginMode } = await import("../../browser/index.js");

      await endLoginMode();

      res.json({ message: "Test flow ended.", success: true });
    } catch(error) {

      LOG.error("Failed to end test flow: %s.", formatError(error));
      res.status(500).json({ error: "Failed to end test: " + formatError(error), success: false });
    }
  });
}
