/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * routes.ts: Channel route handlers for the PrismCast configuration interface.
 */
import type { Channel, ChannelDelta, ChannelSortField, Nullable, StoredChannel } from "../../../types/index.js";
import type { Express, Request, Response } from "express";
import { LOG, formatError, generateChannelKey, parseM3U, sanitizeString, stringifySorted } from "../../../utils/index.js";
import { PREDEFINED_CHANNELS, PREDEFINED_TAGS } from "../../../channels/index.js";
import { VALID_OPTIONAL_COLUMNS, buildChannelTablePatch, buildChannelTableState, generateTagFilterContent, generateTagManagerBody } from "./table.js";
import { VALID_SORT_FIELDS, compareChannelSort, getAllServiceTags, getCanonicalKey, getChannelServiceLabel, getEnabledServices, getResolvedChannel,
  getServiceDisplayName, getServiceGroup, getServiceSelection, getServiceTagForChannel, isServiceTagEnabled, resolvePredefinedVariant,
  resolveServiceKey, setEnabledServices, setServiceSelection } from "../../../config/services.js";
import { filterDefaults, loadUserConfig, saveUserConfig } from "../../../config/userConfig.js";
import { getActiveTagVocabulary, getChannelEffectiveTags, getChannelListing, getEastWithPacificPredefinedKeys, getPacificPredefinedKeys, getPredefinedChannel,
  getPredefinedChannels, getTagRegistry, getUserChannels, isPredefinedChannel, isUserChannel, loadUserChannels, resolveStoredChannel,
  saveServiceSelections, saveTagRegistry, saveUserChannels, setTagRegistry, transformChannelTags, validateChannelKey, validateChannelName,
  validateChannelNumber, validateChannelProfile, validateChannelUrl, validateImportedChannels } from "../../../config/userChannels.js";
import { CONFIG } from "../../../config/index.js";
import type { UserChannel } from "../../../config/userChannels.js";
import { getDomainConfig } from "../../../config/sites.js";
import { getProfiles } from "../../../config/profiles.js";
import { updateChannelLogo } from "../../../streaming/showInfo.js";

// Fields that appear in the generated M3U playlist and affect Channels DVR's view of the channel. Used to decide whether the playlist reload hint is shown.
const M3U_FIELDS = [ "channelNumber", "name", "stationId", "tvgShift" ];

const PLAYLIST_HINT = " Reload the playlist in Channels DVR to see this change.";

/**
 * Builds a service filter warning when a URL's service tag is not in the active filter. Returns undefined when no filter is active, the tag is "direct", or the
 * tag is already enabled. The client shows the returned warning as a toast with a one-click enable action.
 * @param url - The channel URL to derive the service tag from.
 * @returns The warning with service tag and display label, or undefined if no warning is needed.
 */
function buildServiceFilterWarning(url: string): { serviceLabel: string; serviceTag: string } | undefined {

  if(getEnabledServices().length === 0) {

    return undefined;
  }

  const tag = getDomainConfig(url)?.serviceTag;

  if(tag && (tag !== "direct") && !isServiceTagEnabled(tag)) {

    return { serviceLabel: getServiceDisplayName(url), serviceTag: tag };
  }

  return undefined;
}

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
 * Disables one or more predefined channels by adding their keys to the disabledPredefined list in user config. Updates both the config file and the runtime
 * CONFIG object. Shared by the toggle-predefined and bulk-add handlers to avoid duplicating the config load/save/update pattern.
 * @param keys - The predefined channel keys to disable.
 */
async function disablePredefinedChannels(keys: string[]): Promise<void> {

  if(keys.length === 0) {

    return;
  }

  const configResult = await loadUserConfig();
  const userConfig = configResult.config;

  userConfig.channels ??= {};
  userConfig.channels.disabledPredefined ??= [];

  const disabledSet = new Set(userConfig.channels.disabledPredefined);

  for(const key of keys) {

    disabledSet.add(key);
  }

  userConfig.channels.disabledPredefined = [...disabledSet].sort();

  await saveUserConfig(userConfig);

  CONFIG.channels.disabledPredefined = userConfig.channels.disabledPredefined;
}

/**
 * Enables one or more predefined channels by removing their keys from the disabledPredefined list in user config. Updates both the config file and the
 * runtime CONFIG object. Shared by the toggle-predefined and bulk-add handlers to avoid duplicating the config load/save/update pattern.
 * @param keys - The predefined channel keys to enable.
 */
async function enablePredefinedChannels(keys: string[]): Promise<void> {

  if(keys.length === 0) {

    return;
  }

  const configResult = await loadUserConfig();
  const userConfig = configResult.config;

  userConfig.channels ??= {};
  userConfig.channels.disabledPredefined ??= [];

  const disabledSet = new Set(userConfig.channels.disabledPredefined);

  for(const key of keys) {

    disabledSet.delete(key);
  }

  userConfig.channels.disabledPredefined = [...disabledSet].sort();

  await saveUserConfig(userConfig);

  CONFIG.channels.disabledPredefined = userConfig.channels.disabledPredefined;
}

/**
 * Installs all channel-related route handlers on the Express application.
 * @param app - The Express application.
 */
export function setupChannelRoutes(app: Express): void {

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
      res.send(stringifySorted(resolved) + "\n");
    } catch(error) {

      LOG.error("Failed to export channels: %s.", formatError(error));
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

      // Return a full table patch since a bulk JSON import replaces all user channels — every row in the listing may have changed.
      const allKeys = getChannelListing().map((entry) => entry.key);

      res.json({ message: "Imported " + String(channelCount) + " channel" + (channelCount === 1 ? "" : "s") + " successfully.",
        patch: buildChannelTablePatch(allKeys, getProfiles()), success: true });
    } catch(error) {

      LOG.error("Failed to import channels: %s.", formatError(error));
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
      const affectedKeys: string[] = [];
      const conflicts: string[] = [];
      const importErrors: string[] = [];
      const seenKeys = new Set<string>();
      let imported = 0;
      let replaced = 0;
      let skipped = 0;

      // Process each parsed channel. Sanitize string values before validation to strip non-printable characters.
      for(const m3uChannel of parseResult.channels) {

        m3uChannel.name = sanitizeString(m3uChannel.name);
        m3uChannel.url = sanitizeString(m3uChannel.url);
        m3uChannel.stationId &&= sanitizeString(m3uChannel.stationId);

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
        affectedKeys.push(key);
      }

      // Save the updated channels.
      await saveUserChannels(existingChannels);

      // Log the import.
      LOG.info("M3U import completed: %d imported, %d replaced, %d skipped.", imported, replaced, skipped);

      // Build response with patch for in-place UI update.
      res.json({

        conflicts,
        errors: [ ...parseResult.errors, ...importErrors ],
        imported,
        patch: ((imported > 0) || (replaced > 0)) ? buildChannelTablePatch(affectedKeys, getProfiles()) : undefined,
        replaced,
        skipped,
        success: true
      });
    } catch(error) {

      LOG.error("Failed to import M3U channels: %s.", formatError(error));
      res.status(500).json({ error: "Failed to import channels: " + formatError(error), success: false });
    }
  });

  // POST /config/channels/modify - Apply channel modifications from the browse modal. Handles four action types: 'add' creates new user channels, 'enable'
  // re-enables a disabled predefined channel and sets its service, 'switch' changes the service selection for an existing channel, and 'remove' reverts a
  // channel to its canonical service (disabling it if no alternative service is available). Channel writes and service selection changes are batched into
  // single saves to avoid redundant file I/O.
  app.post("/config/channels/modify", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as {
        channels?: {
          action?: string; canonicalKey?: string; channelSelector?: string; name?: string; serviceSlug?: string; stationId?: string; url?: string;
        }[];
      };

      const channels = body.channels;

      // Validate that channels array is provided and non-empty.
      if(!Array.isArray(channels) || (channels.length === 0)) {

        res.status(400).json({ error: "No channels provided.", success: false });

        return;
      }

      // Load existing user channels.
      const loadResult = await loadUserChannels();
      const existingChannels = loadResult.parseError ? {} : loadResult.channels;

      // Build a set of all existing channel keys (predefined + user) for deduplication.
      const allKeys = new Set([ ...Object.keys(PREDEFINED_CHANNELS), ...Object.keys(existingChannels) ]);

      const affectedKeys = new Set<string>();
      const disablePredefined = new Set<string>();
      const errors: string[] = [];
      let added = 0;
      let removed = 0;
      let switched = 0;
      let channelsChanged = false;
      let selectionsChanged = false;

      // Build a UserChannel object from an entry's fields. Shared by the switch/enable and add paths to avoid duplicating channel construction logic. When
      // canonicalKey is provided, the channel is a service variant — identity fields (name, stationId) are omitted because they're resolved from the canonical
      // at runtime via applyVariantInheritance. Standalone channels (no canonicalKey) include all fields.
      function buildUserChannel(entry: { channelSelector?: string; name?: string; stationId?: string; url?: string },
        channelName: string, channelUrl: string, selector: string, canonicalKey?: string): UserChannel {

        const channel: UserChannel = {

          ...(canonicalKey ? { canonicalKey } : { name: channelName }),
          channelSelector: selector || undefined,
          url: channelUrl
        };

        if(!canonicalKey && entry.stationId) {

          channel.stationId = sanitizeString(entry.stationId);
        }

        return channel;
      }

      for(const entry of channels) {

        const action = entry.action ?? "add";
        const name = sanitizeString(entry.name?.trim() ?? "");
        const serviceSlug = entry.serviceSlug?.trim() ?? "";

        // Handle enable and switch. Enable re-enables a disabled predefined channel, then falls through to the switch logic to set the service selection.
        // Switch changes the service selection for an existing channel to point to the browsed service's variant, creating the variant if needed.
        if((action === "switch") || (action === "enable")) {

          const canonicalKey = entry.canonicalKey?.trim() ?? "";

          if(!canonicalKey) {

            errors.push("Entry for '" + name + "' missing canonical key.");

            continue;
          }

          // Re-enable the channel if it was disabled. Must complete before setting the service selection since the config write makes the channel
          // visible in the lineup.
          if(action === "enable") {

            // eslint-disable-next-line no-await-in-loop -- Sequential: config state must be updated before setting service selection.
            await enablePredefinedChannels([canonicalKey]);
          }

          const variantKey = canonicalKey + "-" + serviceSlug;

          // If no variant exists for this service, create one as a user channel. The canonicalKey parameter tells buildUserChannel to produce a variant
          // (service-specific fields only, no identity fields).
          if(!allKeys.has(variantKey)) {

            existingChannels[variantKey] = buildUserChannel(entry, name, sanitizeString(entry.url?.trim() ?? ""),
              sanitizeString(entry.channelSelector?.trim() ?? ""), canonicalKey);
            allKeys.add(variantKey);
            channelsChanged = true;
          }

          setServiceSelection(canonicalKey, variantKey);
          selectionsChanged = true;
          switched++;
          affectedKeys.add(canonicalKey);

          continue;
        }

        // Handle service removal. Reverts the channel away from this service using a three-tier fallback: (1) clear the selection and let
        // resolveServiceKey find the next enabled variant or canonical, (2) if the resolved service is still this service (no alternative exists),
        // disable the predefined channel or delete the user channel.
        if(action === "remove") {

          const canonicalKey = entry.canonicalKey?.trim() ?? "";

          if(!canonicalKey) {

            errors.push("Remove entry for '" + name + "' missing canonical key.");

            continue;
          }

          // Clear the service selection. resolveServiceKey will now return the canonical or the first enabled alternative variant.
          setServiceSelection(canonicalKey, canonicalKey);
          selectionsChanged = true;

          // Check if the resolved service is still this service. If so, there is no alternative — disable or delete the channel.
          const resolvedKey = resolveServiceKey(canonicalKey);
          const resolvedTag = getServiceTagForChannel(resolvedKey);

          if(resolvedTag === serviceSlug) {

            // No alternative service exists. Disable predefined channels or delete user channels.
            if(isPredefinedChannel(canonicalKey)) {

              disablePredefined.add(canonicalKey);
            } else {

              Reflect.deleteProperty(existingChannels, canonicalKey);
              channelsChanged = true;
            }
          }

          removed++;
          affectedKeys.add(canonicalKey);

          continue;
        }

        // Handle add. Creates a new user channel for channels not yet in the lineup. The browse modal sends 'add' only for genuinely new channels (no
        // existing canonical). Channels that match existing canonicals appear as 'switch' state in the modal.
        const url = sanitizeString(entry.url?.trim() ?? "");
        const channelSelector = sanitizeString(entry.channelSelector?.trim() ?? "");

        if(!name) {

          errors.push("Channel entry missing name.");

          continue;
        }

        if(!url) {

          errors.push("Channel '" + name + "' missing URL.");

          continue;
        }

        const urlError = validateChannelUrl(url);

        if(urlError) {

          errors.push("Channel '" + name + "': " + urlError);

          continue;
        }

        const baseKey = generateChannelKey(name);

        if(!baseKey) {

          errors.push("Could not generate key for channel '" + name + "'.");

          continue;
        }

        const canonicalExists = allKeys.has(baseKey);
        const key = (canonicalExists && serviceSlug) ? baseKey + "-" + serviceSlug : baseKey;

        // Skip channels that already exist in the lineup.
        if(allKeys.has(key)) {

          continue;
        }

        // When the key differs from baseKey, this is a service variant — pass baseKey as canonicalKey so buildUserChannel produces a variant with only
        // service-specific fields. Standalone channels (key === baseKey) get the full channel object with identity fields.
        const newChannel = buildUserChannel(entry, name, url, channelSelector, (key !== baseKey) ? baseKey : undefined);

        existingChannels[key] = newChannel;
        allKeys.add(key);
        channelsChanged = true;
        added++;
        affectedKeys.add(canonicalExists ? baseKey : key);
      }

      // Save changes. saveUserChannels() persists both channel data and service selections in a single file write, so we only need saveServiceSelections()
      // (which writes the same file using the module-level cache) when selections changed but no new channels were added.
      if(channelsChanged) {

        await saveUserChannels(existingChannels);
      } else if(selectionsChanged) {

        await saveServiceSelections();
      }

      // Disable predefined channels that had no alternative service after removal.
      await disablePredefinedChannels([...disablePredefined]);

      for(const disabledKey of disablePredefined) {

        affectedKeys.add(disabledKey);
      }

      // Build a descriptive response message.
      const parts: string[] = [];

      if(added > 0) {

        parts.push("Added " + String(added) + " channel" + (added === 1 ? "" : "s") + ".");
      }

      if(switched > 0) {

        parts.push("Switched " + String(switched) + " channel" + (switched === 1 ? "" : "s") + ".");
      }

      if(removed > 0) {

        parts.push("Reverted " + String(removed) + " channel" + (removed === 1 ? "" : "s") + ".");
      }

      const message = (parts.length > 0 ? parts.join(" ") : "No changes made.") + PLAYLIST_HINT;

      LOG.info("Browse channels completed: %d added, %d switched, %d removed.", added, switched, removed);

      // Check if the browsed service isn't in the active filter. All channels in a browse batch share the same service — derive the tag from the first add entry.
      const firstAddUrl = (added > 0) ? channels.find((e) => (e.action === "add"))?.url : undefined;
      const serviceWarning = firstAddUrl ? buildServiceFilterWarning(firstAddUrl) : undefined;

      res.json({ added, errors, message, patch: buildChannelTablePatch([...affectedKeys], getProfiles()), removed, serviceWarning, success: true,
        switched });
    } catch(error) {

      LOG.error("Failed to apply browse changes: %s.", formatError(error));
      res.status(500).json({ error: "Failed to apply changes: " + formatError(error), success: false });
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

      // Enable or disable the channel using the shared helpers that handle config file I/O and runtime CONFIG update.
      if(enabled) {

        await enablePredefinedChannels([key]);
      } else {

        await disablePredefinedChannels([key]);
      }

      LOG.info("Predefined channel '%s' %s.", key, enabled ? "enabled" : "disabled");

      res.json({ enabled, key, patch: buildChannelTablePatch([key], getProfiles()), success: true });
    } catch(error) {

      LOG.error("Failed to toggle predefined channel: %s.", formatError(error));
      res.status(500).json({ error: "Failed to toggle channel: " + formatError(error), success: false });
    }
  });

  // POST /config/service - Update service selection for a multi-service channel.
  app.post("/config/service", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { channel?: string; service?: string };
      const channelKey = body.channel?.trim();
      const serviceKey = body.service?.trim();

      // Validate channel key is provided.
      if(!channelKey) {

        res.status(400).json({ error: "Channel key is required.", success: false });

        return;
      }

      // Validate service key is provided.
      if(!serviceKey) {

        res.status(400).json({ error: "Service key is required.", success: false });

        return;
      }

      // Canonicalize the channel key to ensure selections are stored under the canonical key, not variant keys.
      const canonicalKey = getCanonicalKey(channelKey);

      // Validate the channel has service options.
      const serviceGroup = getServiceGroup(canonicalKey);

      if(!serviceGroup) {

        res.status(400).json({ error: "Channel '" + canonicalKey + "' does not have multiple services.", success: false });

        return;
      }

      // Validate the service key is valid for this channel.
      const validServiceKeys = serviceGroup.variants.map((v) => v.key);

      if(!validServiceKeys.includes(serviceKey)) {

        res.status(400).json({ error: "Invalid service '" + serviceKey + "' for channel '" + canonicalKey + "'.", success: false });

        return;
      }

      // Update the service selection.
      setServiceSelection(canonicalKey, serviceKey);

      // Save to disk.
      await saveServiceSelections();

      // Resolve display names for logging before generating the row HTML.
      const canonicalChannel = getResolvedChannel(canonicalKey);
      const variantChannel = getResolvedChannel(serviceKey);
      const channelName = canonicalChannel?.name ?? canonicalKey;
      const serviceLabel = variantChannel ? getChannelServiceLabel(variantChannel) : serviceKey;

      LOG.info("Service for %s changed to %s.", channelName, serviceLabel);

      // Return a channel table patch so the client can update the row and summary counts in place.
      const profiles = getProfiles();

      res.json({ channel: canonicalKey, patch: buildChannelTablePatch([canonicalKey], profiles), service: serviceKey, success: true });
    } catch(error) {

      LOG.error("Failed to update service selection: %s.", formatError(error));
      res.status(500).json({ error: "Failed to update service: " + formatError(error), success: false });
    }
  });

  // POST /config/channels/bulk-toggle-predefined - Toggle predefined channels by scope (all, pacific, east).
  app.post("/config/channels/bulk-toggle-predefined", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { enabled?: boolean; scope?: string };
      const enabled = body.enabled;
      const scope = body.scope;

      // Validate enabled is provided.
      if(typeof enabled !== "boolean") {

        res.status(400).json({ error: "Enabled state (true/false) is required.", success: false });

        return;
      }

      // Validate scope is provided and recognized.
      if((scope !== "all") && (scope !== "pacific") && (scope !== "east")) {

        res.status(400).json({ error: "Scope must be 'all', 'pacific', or 'east'.", success: false });

        return;
      }

      // Compute the target key set based on scope.
      let targetKeys: string[];

      switch(scope) {

        case "pacific": {

          targetKeys = getPacificPredefinedKeys();

          break;
        }

        case "east": {

          targetKeys = getEastWithPacificPredefinedKeys();

          break;
        }

        default: {

          targetKeys = Object.keys(getPredefinedChannels());

          break;
        }
      }

      // Load current config.
      const configResult = await loadUserConfig();
      const userConfig = configResult.config;

      // Initialize channels.disabledPredefined if not present.
      userConfig.channels ??= {};

      const targetSet = new Set(targetKeys);

      if(enabled && (scope === "all")) {

        // Enable all is a full reset — clear the entire disabled list.
        userConfig.channels.disabledPredefined = [];
      } else if(enabled) {

        // Scoped enable: remove target keys from the disabled list (subtractive — preserves other disabled channels).
        userConfig.channels.disabledPredefined = (userConfig.channels.disabledPredefined ?? []).filter((k) => !targetSet.has(k));
      } else {

        // Disable: add target keys to the disabled list (additive — preserves other disabled channels).
        const existing = new Set(userConfig.channels.disabledPredefined ?? []);

        for(const k of targetKeys) {

          existing.add(k);
        }

        userConfig.channels.disabledPredefined = [...existing].sort();
      }

      await saveUserConfig(userConfig);

      // Update the runtime CONFIG to reflect the change immediately.
      CONFIG.channels.disabledPredefined = userConfig.channels.disabledPredefined;

      const affected = targetKeys.length;
      const scopeLabel = (scope === "all") ? "All" : (scope === "pacific") ? "Pacific" : "East";

      LOG.info("%s predefined channels %s (%d affected).", scopeLabel, enabled ? "enabled" : "disabled", affected);

      res.json({ affected, enabled, keys: targetKeys, patch: buildChannelTablePatch(targetKeys, getProfiles()), scope, success: true });
    } catch(error) {

      LOG.error("Failed to toggle predefined channels: %s.", formatError(error));
      res.status(500).json({ error: "Failed to toggle channels: " + formatError(error), success: false });
    }
  });

  // POST /config/channels/auto-number - Assign sequential channel numbers to visible channels in the current sort order, or clear all channel numbers when
  // start is 0. Overwrites existing channel numbers for affected channels. Only channels that are enabled and available by service filter are affected.
  app.post("/config/channels/auto-number", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { sortDirection?: string; sortField?: string; start?: number };
      const start = (typeof body.start === "number") ? body.start : 1;
      const clearMode = (start === 0);
      const sortField: ChannelSortField = (body.sortField as ChannelSortField | undefined) ?? "name";
      const sortDir = (body.sortDirection === "desc") ? "desc" : "asc";

      if(!clearMode && ((start < 1) || (start > 99999))) {

        res.status(400).json({ error: "Starting number must be between 1 and 99999.", success: false });

        return;
      }

      if(!VALID_SORT_FIELDS.has(sortField)) {

        res.status(400).json({ error: "Invalid sort field.", success: false });

        return;
      }

      // Get visible channels (enabled + available by service filter) sorted by the user's current sort order.
      const listing = getChannelListing().filter((entry) => entry.enabled && entry.availableByService);

      listing.sort((a, b) => compareChannelSort(a.channel, a.key, b.channel, b.key, sortField, sortDir));

      // Load user channels for modification.
      const result = await loadUserChannels();

      if(result.parseError) {

        res.status(400).json({ error: "Cannot auto-number: channels file contains invalid JSON.", success: false });

        return;
      }

      const affectedKeys: string[] = [];

      if(clearMode) {

        // Clear channel numbers from all visible channels. Null signals "clear this field" — the normalizer in saveUserChannels() handles the storage conventions.
        for(const entry of listing) {

          const existing = result.channels[entry.key] ?? {};

          existing.channelNumber = null;
          result.channels[entry.key] = existing;
          affectedKeys.push(entry.key);
        }
      } else {

        // Assign sequential numbers starting from the requested start value.
        for(let i = 0; i < listing.length; i++) {

          const entry = listing[i];
          const num = start + i;

          if(num > 99999) {

            break;
          }

          const existing = result.channels[entry.key] ?? {};

          existing.channelNumber = num;
          result.channels[entry.key] = existing;
          affectedKeys.push(entry.key);
        }
      }

      await saveUserChannels(result.channels);

      const action = clearMode ? "Cleared" : "Numbered";

      LOG.info("%s channel numbers for %d channels.", action, affectedKeys.length);

      const profiles = getProfiles();

      const message = clearMode ?
        "Cleared channel numbers from " + String(affectedKeys.length) + " channels." + PLAYLIST_HINT :
        "Numbered " + String(affectedKeys.length) + " channels." + PLAYLIST_HINT;

      res.json({ affected: affectedKeys.length, message, patch: buildChannelTablePatch(affectedKeys, profiles), success: true });
    } catch(error) {

      LOG.error("Failed to auto-number channels: %s.", formatError(error));
      res.status(500).json({ error: "Failed to auto-number channels: " + formatError(error), success: false });
    }
  });

  // POST /config/channels/hdhr-bulk - Toggle HDHomeRun lineup inclusion for all visible channels. When enable is true, all channels are included (hdhrEnabled
  // cleared). When false, all channels are excluded (hdhrEnabled set to false).
  app.post("/config/channels/hdhr-bulk", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { enable?: boolean };
      const enable = body.enable === true;

      // Get visible channels (enabled + available by service filter).
      const listing = getChannelListing().filter((entry) => entry.enabled && entry.availableByService);

      // Load user channels for modification.
      const result = await loadUserChannels();

      if(result.parseError) {

        res.status(400).json({ error: "Cannot update HDHR settings: channels file contains invalid JSON.", success: false });

        return;
      }

      const affectedKeys: string[] = [];

      for(const entry of listing) {

        const current = entry.channel.hdhrEnabled !== false;

        // Skip channels that already match the target state.
        if(current === enable) {

          continue;
        }

        const existing = result.channels[entry.key] ?? {};

        existing.hdhrEnabled = enable ? null : false;
        result.channels[entry.key] = existing;
        affectedKeys.push(entry.key);
      }

      if(affectedKeys.length === 0) {

        res.json({ affected: 0, message: "No changes needed.", success: true });

        return;
      }

      await saveUserChannels(result.channels);

      const action = enable ? "included in" : "excluded from";

      LOG.info("Bulk HDHR toggle: %d channels %s HDHR lineup.", affectedKeys.length, action);

      const profiles = getProfiles();
      const message = String(affectedKeys.length) + " channel(s) " + action + " the HDHomeRun lineup.";

      res.json({ affected: affectedKeys.length, message, patch: buildChannelTablePatch(affectedKeys, profiles), success: true });
    } catch(error) {

      LOG.error("Failed to bulk-toggle HDHR: %s.", formatError(error));
      res.status(500).json({ error: "Failed to toggle HDHR settings: " + formatError(error), success: false });
    }
  });

  // POST /config/channels/bulk-tags - Add or remove a tag on all enabled, service-available channels. Operates on the same channel set as other bulk actions.
  // transformChannelTags handles loading, delta normalization, and persistence. Returns a channel table patch for all affected rows and an updated tag manager
  // modal body.
  app.post("/config/channels/bulk-tags", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { action?: string; tag?: string };
      const action = body.action;
      const tag = typeof body.tag === "string" ? body.tag.trim().toLowerCase() : "";

      if((action !== "add") && (action !== "remove")) {

        res.status(400).json({ error: "Action must be 'add' or 'remove'.", success: false });

        return;
      }

      if(!tag) {

        res.status(400).json({ error: "Tag is required.", success: false });

        return;
      }

      // Validate the tag is in the active vocabulary.
      const vocabulary = new Set(getActiveTagVocabulary());

      if(!vocabulary.has(tag)) {

        res.status(400).json({ error: "Unknown tag: " + tag + ".", success: false });

        return;
      }

      const { affectedKeys, error } = await transformChannelTags(
        (entry) => entry.enabled && entry.availableByService,
        (tags) => (action === "add") ? (tags.includes(tag) ? tags : [ ...tags, tag ]) : tags.filter((t) => t !== tag)
      );

      if(error) {

        res.status(400).json({ error, success: false });

        return;
      }

      if(affectedKeys.length === 0) {

        res.json({ affected: 0, message: "No changes needed.", success: true });

        return;
      }

      const verb = (action === "add") ? "added to" : "removed from";

      LOG.info("Bulk tag %s: %s on %d channels.", action, tag, affectedKeys.length);

      const profiles = getProfiles();
      const message = "Tag '" + tag + "' " + verb + " " + String(affectedKeys.length) + " channel(s).";

      res.json({

        affected: affectedKeys.length,
        filterContent: generateTagFilterContent(),
        message,
        modalBody: generateTagManagerBody(),
        patch: buildChannelTablePatch(affectedKeys, profiles),
        success: true
      });
    } catch(error) {

      LOG.error("Failed to bulk-update tags: %s.", formatError(error));
      res.status(500).json({ error: "Failed to update tags: " + formatError(error), success: false });
    }
  });

  // POST /config/service-filter - Update the service filter (enabled service tags).
  app.post("/config/service-filter", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { enabledServices?: string[] };
      const tags = body.enabledServices;

      // Validate tags is an array.
      if(!Array.isArray(tags)) {

        res.status(400).json({ error: "enabledServices must be an array.", success: false });

        return;
      }

      // Validate all tags are known. Tags already in enabledServices are accepted even if no current channel or profile produces them — this allows stale tags to be
      // removed via the UI without blocking the request.
      const knownTags = new Set(getAllServiceTags().map((t) => t.tag));
      const currentTags = new Set(getEnabledServices());

      for(const tag of tags) {

        if(!knownTags.has(tag) && !currentTags.has(tag)) {

          res.status(400).json({ error: "Unknown service tag: " + tag, success: false });

          return;
        }
      }

      // Update module-level state.
      setEnabledServices(tags);

      // Update runtime CONFIG.
      CONFIG.channels.enabledServices = [...tags];

      // Save to config file.
      const configResult = await loadUserConfig();
      const userConfig = configResult.config;

      userConfig.channels ??= {};
      userConfig.channels.enabledServices = tags;

      await saveUserConfig(filterDefaults(userConfig));

      LOG.info("Service filter updated: %s.", tags.length > 0 ? tags.join(", ") : "all services");

      // Return counts patch so the client can update summary counts after filter change. No rows — the client applies CSS visibility changes itself.
      const { counts, scopeCounts } = buildChannelTableState();

      res.json({ enabledServices: tags, patch: { counts, rows: [], scopeCounts }, success: true });
    } catch(error) {

      LOG.error("Failed to update service filter: %s.", formatError(error));
      res.status(500).json({ error: "Failed to update service filter: " + formatError(error), success: false });
    }
  });

  // POST /config/channels/setup-completed - Mark the Service Setup flow as completed. Called when the wizard finishes or the user explicitly skips.
  app.post("/config/channels/setup-completed", async (_req: Request, res: Response): Promise<void> => {

    try {

      CONFIG.channels.setupCompleted = true;

      const configResult = await loadUserConfig();

      configResult.config.channels ??= {};
      configResult.config.channels.setupCompleted = true;

      await saveUserConfig(configResult.config);

      // Return a full patch so the client can refresh the channel table after the setup wizard completes. The wizard's browse step may have added channels
      // via the modify endpoint, and the service filter may have changed — the patch ensures the table reflects the current state.
      const { counts, scopeCounts } = buildChannelTableState();

      res.json({ patch: { counts, rows: [], scopeCounts }, success: true });
    } catch(error) {

      LOG.error("Failed to save setup completed state: %s.", formatError(error));
      res.status(500).json({ error: "Failed to save setup state: " + formatError(error), success: false });
    }
  });

  // POST /config/channels/display-prefs - Update channel table display preferences (visible columns, sort field, sort direction).
  app.post("/config/channels/display-prefs", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { sortDirection?: string; sortField?: string; visibleColumns?: string[] };

      // Validate and apply visible columns if provided.
      if(body.visibleColumns !== undefined) {

        if(!Array.isArray(body.visibleColumns)) {

          res.status(400).json({ error: "visibleColumns must be an array.", success: false });

          return;
        }

        for(const col of body.visibleColumns) {

          if(!VALID_OPTIONAL_COLUMNS.has(col)) {

            res.status(400).json({ error: "Unknown column: " + col, success: false });

            return;
          }
        }

        CONFIG.channels.visibleColumns = [...body.visibleColumns];
      }

      // Validate and apply sort field if provided.
      if(body.sortField !== undefined) {

        if(!VALID_SORT_FIELDS.has(body.sortField as ChannelSortField)) {

          res.status(400).json({ error: "Unknown sort field: " + body.sortField, success: false });

          return;
        }

        CONFIG.channels.channelSortField = body.sortField as ChannelSortField;
      }

      // Validate and apply sort direction if provided.
      if(body.sortDirection !== undefined) {

        if((body.sortDirection !== "asc") && (body.sortDirection !== "desc")) {

          res.status(400).json({ error: "sortDirection must be \"asc\" or \"desc\".", success: false });

          return;
        }

        CONFIG.channels.channelSortDirection = body.sortDirection;
      }

      // Persist to config file.
      const configResult = await loadUserConfig();
      const userConfig = configResult.config;

      userConfig.channels ??= {};
      userConfig.channels.channelSortDirection = CONFIG.channels.channelSortDirection;
      userConfig.channels.channelSortField = CONFIG.channels.channelSortField;
      userConfig.channels.visibleColumns = CONFIG.channels.visibleColumns;

      await saveUserConfig(filterDefaults(userConfig));

      res.json({ success: true });
    } catch(error) {

      LOG.error("Failed to update display preferences: %s.", formatError(error));
      res.status(500).json({ error: "Failed to update display preferences: " + formatError(error), success: false });
    }
  });

  // POST /config/service-bulk-assign - Set all channels to a specific service.
  app.post("/config/service-bulk-assign", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { service?: string };
      const serviceTag = body.service?.trim();

      // Validate service tag.
      if(!serviceTag) {

        res.status(400).json({ error: "Service tag is required.", success: false });

        return;
      }

      let affected = 0;
      const previousSelections: Record<string, Nullable<string>> = {};
      const selections: Record<string, { profile: Nullable<string>; variant: string }> = {};

      // Iterate all channels and set those with a matching variant.
      const listing = getChannelListing();

      for(const entry of listing) {

        const group = getServiceGroup(entry.key);

        if(!group || (group.variants.length <= 1)) {

          continue;
        }

        // Find a variant matching the requested service tag.
        const matchingVariant = group.variants.find((v) => (getServiceTagForChannel(v.key) === serviceTag));

        if(matchingVariant) {

          // Snapshot the current selection before overwriting so the client can offer undo.
          const currentVariant = getServiceSelection(entry.key);

          previousSelections[entry.key] = currentVariant ?? null;

          setServiceSelection(entry.key, matchingVariant.key);
          affected++;

          // Collect the resolved profile name for client-side UI update.
          const resolvedChannel = getResolvedChannel(matchingVariant.key);

          selections[entry.key] = { profile: resolvedChannel?.profile ?? null, variant: matchingVariant.key };
        }
      }

      // Save to disk.
      await saveServiceSelections();

      LOG.info("Bulk assign to '%s': %d of %d channels affected.", serviceTag, affected, listing.length);

      res.json({ affected, previousSelections, selections, success: true, total: listing.length });
    } catch(error) {

      LOG.error("Failed to bulk assign service: %s.", formatError(error));
      res.status(500).json({ error: "Failed to bulk assign service: " + formatError(error), success: false });
    }
  });

  // POST /config/service-bulk-restore - Restore previous service selections (undo bulk assign).
  app.post("/config/service-bulk-restore", async (req: Request, res: Response): Promise<void> => {

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

        const group = getServiceGroup(key);

        if(!group) {

          continue;
        }

        // A null value means the channel was using the default (canonical) selection. Restoring by setting the selection to the canonical key clears the override.
        if(variantKey === null) {

          setServiceSelection(key, key);

        } else {

          // Validate the variant belongs to this channel's service group before restoring.
          const isValid = group.variants.some((v) => (v.key === variantKey));

          if(!isValid) {

            continue;
          }

          setServiceSelection(key, variantKey);
        }

        restored++;

        // Build the same selection response format as bulk assign for client-side UI updates.
        const effectiveKey = variantKey ?? key;
        const resolvedChannel = getResolvedChannel(effectiveKey);

        selections[key] = { profile: resolvedChannel?.profile ?? null, variant: effectiveKey };
      }

      // Save to disk.
      await saveServiceSelections();

      LOG.info("Bulk restore: %d channel(s) reverted.", restored);

      res.json({ restored, selections, success: true });
    } catch(error) {

      LOG.error("Failed to bulk restore services: %s.", formatError(error));
      res.status(500).json({ error: "Failed to bulk restore services: " + formatError(error), success: false });
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

        res.json({ key, message: "Channel '" + key + "' reverted to defaults." + revertHint,
          patch: buildChannelTablePatch([key], profiles), success: true });

        return;
      }

      // Handle inline-edit action — update a single field on an existing channel without requiring all fields. Used by the inline cell editing UI for channel
      // number, station ID, and HDHR lineup toggles.
      if(action === "inline-edit") {

        if(!key) {

          res.status(400).json({ message: "Channel key is required.", success: false });

          return;
        }

        const field = body.field;
        const value = sanitizeString(body.value ?? "");

        if((field !== "channelNumber") && (field !== "hdhrEnabled") && (field !== "stationId") && (field !== "tags")) {

          res.status(400).json({ message: "Invalid inline-edit field.", success: false });

          return;
        }

        // Validate channel number if the field is channelNumber.
        if(field === "channelNumber") {

          const numberError = validateChannelNumber(value, key);

          if(numberError) {

            res.status(400).json({ message: numberError, success: false });

            return;
          }
        }

        // Load and update the channel. For predefined channels, store a delta override with null to clear fields. For user channels, use undefined to delete.
        const result = await loadUserChannels();

        if(result.parseError) {

          res.status(400).json({ message: "Cannot edit: channels file contains invalid JSON.", success: false });

          return;
        }

        // Use a StoredChannelMap-compatible update. The ?? {} creates an empty ChannelDelta for predefined channels that don't have a user override yet. Null signals
        // "clear this field" for all channel types — the normalizer in saveUserChannels() handles delta comparison for predefined channels and null-stripping for
        // user channels.
        const stored = result.channels[key] ?? {};

        if(field === "channelNumber") {

          stored.channelNumber = value ? parseInt(value, 10) : null;
        } else if(field === "hdhrEnabled") {

          stored.hdhrEnabled = (value === "false") ? false : null;
        } else if(field === "tags") {

          const tags = value ? [...new Set(value.split(",").map((t) => t.trim().toLowerCase()).filter((t) => t.length > 0))].sort() : [];

          (stored as Record<string, unknown>).tags = (tags.length > 0) ? tags : null;
        } else {

          stored.stationId = value || null;
        }

        result.channels[key] = stored;

        await saveUserChannels(result.channels);

        const fieldLabels: Record<string, string> = {

          channelNumber: "Channel number", hdhrEnabled: "HDHR lineup", stationId: "Station ID", tags: "Tags"
        };

        const fieldLabel = fieldLabels[field] ?? field;
        const displayValue = (field === "hdhrEnabled") ? ((value === "false") ? "excluded" : "included") : (value || "(cleared)");

        LOG.info("Inline edit: %s for '%s' set to '%s'.", fieldLabel, key, displayValue);

        const playlistHint = (field === "hdhrEnabled") ? "" : PLAYLIST_HINT;

        res.json({ key, message: fieldLabel + " updated." + playlistHint, patch: buildChannelTablePatch([key], profiles), success: true });

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

        // Return a patch. If a predefined channel exists with the same key, the patch includes an "update" row for the predefined version (so the client replaces
        // the user channel row with the predefined original). Otherwise, the patch includes a "remove" action for the deleted key.
        res.json({ key, message: "Channel '" + key + "' deleted successfully." + PLAYLIST_HINT,
          patch: buildChannelTablePatch([key], profiles), success: true });

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

      // Collect and sanitize form values. sanitizeString() strips non-printable characters and trims whitespace.
      const name = sanitizeString(body.name ?? "");
      const url = sanitizeString(body.url ?? "");
      const profile = sanitizeString(body.profile ?? "");
      const stationId = sanitizeString(body.stationId ?? "");
      const channelSelector = sanitizeString(body.channelSelector ?? "");
      const channelNumberStr = sanitizeString(body.channelNumber ?? "");
      const hdhrEnabled = body.hdhrEnabled !== "false";

      // Parse tags from comma-separated input. Trim, lowercase, filter empty, sort, deduplicate.
      const tagsRaw = sanitizeString(body.tags ?? "");
      const tags = tagsRaw ? [...new Set(tagsRaw.split(",").map((t) => t.trim().toLowerCase()).filter((t) => t.length > 0))].sort() : [];

      // Validate channel number if provided.
      const channelNumberError = validateChannelNumber(channelNumberStr, key);

      if(channelNumberError) {

        formErrors.channelNumber = channelNumberError;
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
        // computation, so adding a new form field only requires adding it here. String fields use "" for empty; channelNumber uses undefined; hdhrEnabled uses boolean.
        const formValues: Record<string, boolean | string | number | null | undefined> = {

          channelNumber: channelNumberStr ? parseInt(channelNumberStr, 10) : undefined,
          channelSelector,
          hdhrEnabled,
          name,
          profile,
          stationId,
          url
        };

        // Helper to read a comparable value from a Channel object. String fields default to "" when undefined so they match the form's empty-string representation.
        // channelNumber stays as number | undefined since the form value uses the same representation. hdhrEnabled defaults to true when absent.
        const channelValue = (ch: Channel, field: string): boolean | string | number | undefined => {

          const val = (ch as unknown as Record<string, unknown>)[field];

          if(field === "hdhrEnabled") {

            return (val as boolean | undefined) !== false;
          }

          return (field === "channelNumber") ? val as number | undefined : (val as string | undefined) ?? "";
        };

        // First check: did the user change anything from what the form showed? The edit form is pre-populated with the selected service's resolved channel, which
        // may differ from the canonical predefined base when a variant is selected (e.g., the Hulu variant has a different URL and channelSelector). If the submitted
        // values match the displayChannel exactly, the user saved without modification — no override should be created, and any existing override is preserved.
        // Tags are compared separately (array comparison) after the scalar fields.
        const resolvedKey = resolveServiceKey(key);
        const displayChannel = getResolvedChannel(resolvedKey) ?? predefinedBase;

        const displayTags = getChannelEffectiveTags(displayChannel);
        const scalarUnchanged = Object.keys(formValues).every((field) => formValues[field] === channelValue(displayChannel, field));
        const tagsUnchanged = JSON.stringify(tags) === JSON.stringify(displayTags);

        if(scalarUnchanged && tagsUnchanged) {

          res.json({ key, message: "No changes to save.", success: true });

          return;
        }

        // Second check: compute a delta against the canonical predefined base. This determines whether the user's changes create a custom override or effectively
        // revert the channel to predefined defaults. Changed fields store their new value; empty/undefined fields store null (explicit clear). Tags are handled
        // separately from the scalar loop since they require array comparison.
        const delta: ChannelDelta = {};
        let hasChanges = false;

        for(const field of Object.keys(formValues)) {

          if(formValues[field] !== channelValue(predefinedBase, field)) {

            const formVal = formValues[field];

            (delta as Record<string, boolean | string | number | null | undefined>)[field] = ((formVal === "") || (formVal === undefined)) ? null : formVal;
            hasChanges = true;
          }
        }

        // Tags delta: compare the submitted tags against the predefined base's vocabulary-filtered tags. Using effective tags (not raw) ensures that editing an
        // unrelated field while a predefined tag is deleted from the vocabulary doesn't spuriously bake the deletion into the channel's stored delta.
        const predefinedEffectiveTags = getChannelEffectiveTags(predefinedBase);

        if(JSON.stringify(tags) !== JSON.stringify(predefinedEffectiveTags)) {

          delta.tags = (tags.length > 0) ? tags : null;
          hasChanges = true;
        }

        // Helper to check if form values (scalars + tags) match a given channel's properties. Tags use vocabulary-filtered comparison so variant matching works
        // correctly when predefined tags are deleted from the vocabulary.
        const formMatchesChannel = (ch: Channel): boolean => Object.keys(formValues).every((field) => formValues[field] === channelValue(ch, field)) &&
          (JSON.stringify(tags) === JSON.stringify(getChannelEffectiveTags(ch)));

        if(!hasChanges) {

          // The submitted values match the predefined base exactly. If an override exists, this means the user edited their customizations away — treat it as an
          // implicit revert by removing the override and returning the predefined row HTML.
          if(isUserChannel(key)) {

            const implicitRevertHint = playlistHintForStored(result.channels[key]);

            Reflect.deleteProperty(result.channels, key);

            await saveUserChannels(result.channels);

            LOG.info("Channel '%s' reverted to predefined defaults (edit matched predefined values).", key);

            res.json({ key, message: "Channel '" + key + "' reverted to defaults." + implicitRevertHint,
              patch: buildChannelTablePatch([key], profiles), success: true });

            return;
          }

          res.json({ key, message: "No changes to save.", success: true });

          return;
        }

        // The delta has changes vs the canonical predefined. Before storing a custom override, check if the form values match any service variant's predefined
        // definition. This handles the case where a user edits from a variant (e.g., Hulu), makes a change, saves (creating a custom override), then edits again
        // and reverts the change. The URL and channelSelector still differ from the canonical predefined but match the variant — that's a revert to the variant,
        // not a new customization. We resolve each variant against pure PREDEFINED data (not the user-overridden channelsRef) to avoid contamination from the
        // current override.
        const serviceGroup = getServiceGroup(key);
        let matchedVariantKey: string | undefined;

        if(serviceGroup && isUserChannel(key)) {

          for(const variant of serviceGroup.variants) {

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
          setServiceSelection(key, matchedVariantKey);

          await saveUserChannels(result.channels);

          LOG.info("Channel '%s' reverted to variant '%s' (edit matched variant values).", key, matchedVariantKey);

          res.json({ key, message: "Channel '" + key + "' reverted to defaults." + variantRevertHint,
            patch: buildChannelTablePatch([key], profiles), success: true });

          return;
        }

        // No variant match — store the delta and switch the service selection to the canonical key (the custom override). This ensures the service dropdown shows
        // "Custom" after saving, which is the expected behavior when a user customizes a predefined channel.
        setServiceSelection(key, key);
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

        if(tags.length > 0) {

          channel.tags = tags;
        }

        // Only store hdhrEnabled when explicitly disabled. Absent = true (included in HDHR lineup by default).
        if(!hdhrEnabled) {

          channel.hdhrEnabled = false;
        }

        result.channels[key] = channel;

        // For adds the playlist always changes. For edits, check whether any M3U-relevant field differs from the old stored values.
        playlistChanged = (action === "add") ||
          ((oldChannel !== undefined) &&
            M3U_FIELDS.some((f) => (channel as unknown as Record<string, unknown>)[f] !== (oldChannel as unknown as Record<string, unknown>)[f]));
      }

      await saveUserChannels(result.channels);

      // Trigger a logo lookup for the new or updated station ID. Fire-and-forget - the logo appears on the next page load.
      if(stationId) {

        updateChannelLogo(name || key, stationId);
      }

      const actionLabel = (action === "add") ? "added" : "updated";

      LOG.info("User channel '%s' %s.", key, actionLabel);

      // Generate HTML for the channel row so the client can update the DOM without a full page reload.
      // Append a playlist reload hint when the change affects M3U content that Channels DVR consumes.
      const playlistHint = playlistChanged ? PLAYLIST_HINT : "";

      // Check if the new channel's service isn't in the active filter.
      const serviceWarning = (action === "add") ? buildServiceFilterWarning(url) : undefined;

      // Return success response with patch for client-side DOM update. Changes take effect immediately due to hot-reloading in saveUserChannels().
      res.json({

        isNew: action === "add",
        key,
        message: "Channel '" + key + "' " + actionLabel + " successfully." + playlistHint,
        patch: buildChannelTablePatch([key], profiles),
        serviceWarning,
        success: true
      });
    } catch(error) {

      LOG.error("Failed to save channel: %s.", formatError(error));
      res.status(500).json({ message: "Failed to save channel: " + formatError(error), success: false });
    }
  });

  // Tag Management Endpoints.

  // GET /config/tags - Returns the tag vocabulary and registry state. The active vocabulary is the computed merge of predefined tags (minus deleted) plus user
  // tags. The registry contains the raw user state for the tag management UI.
  app.get("/config/tags", (_req: Request, res: Response): void => {

    res.json({

      active: getActiveTagVocabulary(),
      predefined: [...PREDEFINED_TAGS],
      registry: getTagRegistry(),
      success: true
    });
  });

  // POST /config/tags - Create a new user tag. Validates the tag name (lowercase alphanumeric + hyphens, max 30 chars) and checks for duplicates against the
  // active vocabulary and deleted predefined tags (which should be restored, not re-created).
  app.post("/config/tags", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { tag?: string };
      const tag = typeof body.tag === "string" ? body.tag.trim().toLowerCase() : "";

      if(tag.length === 0) {

        res.status(400).json({ error: "Tag name is required.", success: false });

        return;
      }

      if(tag.length > 30) {

        res.status(400).json({ error: "Tag name must be 30 characters or less.", success: false });

        return;
      }

      if(!(/^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/).test(tag)) {

        res.status(400).json({ error: "Tag name must start and end with a letter or number, and contain only lowercase letters, numbers, and hyphens.", success: false });

        return;
      }

      // Check if the tag already exists in the active vocabulary.
      const vocabulary = new Set(getActiveTagVocabulary());

      if(vocabulary.has(tag)) {

        res.status(409).json({ error: "Tag '" + tag + "' already exists.", success: false });

        return;
      }

      // Check if the tag is a deleted predefined tag — the user should restore it instead of creating a duplicate.
      const registry = getTagRegistry();

      if(registry.deletedTags.includes(tag)) {

        res.status(409).json({ error: "Tag '" + tag + "' is a deleted predefined tag. Use restore instead of creating a new one.", success: false });

        return;
      }

      // Add the tag to the user registry and persist. No channels are affected (new tag has no assignments yet).
      registry.tags.push(tag);
      setTagRegistry(registry);

      await saveTagRegistry();

      LOG.info("Created tag: %s.", tag);

      res.json({

        active: getActiveTagVocabulary(), filterContent: generateTagFilterContent(), modalBody: generateTagManagerBody(),
        registry: getTagRegistry(), success: true
      });
    } catch(error) {

      LOG.error("Failed to create tag: %s.", formatError(error));
      res.status(500).json({ error: "Failed to create tag: " + formatError(error), success: false });
    }
  });

  // DELETE /config/tags/:tag - Delete a tag from the vocabulary and cascade to all channel assignments. For predefined tags, the tag is added to deletedTags. For
  // user-created tags, the tag is removed from the user registry. In both cases, the tag is stripped from every channel that has it — predefined channels via delta
  // override, user channels via direct modification. This ensures re-creating a tag with the same name starts fresh with no ghost assignments.
  app.delete("/config/tags/:tag", async (req: Request, res: Response): Promise<void> => {

    try {

      const tag = (req.params as { tag?: string }).tag?.toLowerCase() ?? "";

      if(tag.length === 0) {

        res.status(400).json({ error: "Tag name is required.", success: false });

        return;
      }

      const registry = getTagRegistry();
      const isPredefined = PREDEFINED_TAGS.includes(tag);
      const isUserTag = registry.tags.includes(tag);

      if(!isPredefined && !isUserTag) {

        res.status(404).json({ error: "Tag '" + tag + "' not found.", success: false });

        return;
      }

      if(isPredefined) {

        // Already deleted — no-op, return current state without unnecessary I/O.
        if(registry.deletedTags.includes(tag)) {

          res.json({

            active: getActiveTagVocabulary(), filterContent: generateTagFilterContent(), modalBody: generateTagManagerBody(),
            registry: getTagRegistry(), success: true
          });

          return;
        }

        registry.deletedTags.push(tag);
      } else {

        // Remove from user tags.
        registry.tags = registry.tags.filter((t) => t !== tag);
      }

      setTagRegistry(registry);

      // Cascade: strip the deleted tag from all channel assignments. transformChannelTags handles loading, delta normalization, and persistence.
      const { affectedKeys, error } = await transformChannelTags(
        (entry) => entry.channel.tags?.includes(tag) === true,
        (tags) => tags.filter((t) => t !== tag)
      );

      if(error) {

        res.status(400).json({ error, success: false });

        return;
      }

      LOG.info("Deleted tag '%s' from vocabulary and %d channel assignments.", tag, affectedKeys.length);

      const profiles = getProfiles();

      res.json({

        active: getActiveTagVocabulary(),
        filterContent: generateTagFilterContent(), modalBody: generateTagManagerBody(),
        patch: (affectedKeys.length > 0) ? buildChannelTablePatch(affectedKeys, profiles) : undefined,
        registry: getTagRegistry(),
        success: true
      });
    } catch(error) {

      LOG.error("Failed to delete tag: %s.", formatError(error));
      res.status(500).json({ error: "Failed to delete tag: " + formatError(error), success: false });
    }
  });

  // POST /config/tags/restore - Restore a previously deleted predefined tag. Removes it from deletedTags so it reappears in the active vocabulary, then
  // cascade-restores the tag on predefined channels whose definition includes it. This reverses the cascade delete — predefined channels go back to their default
  // tag assignments. User channels are not affected (their tag assignments were permanently removed during cascade delete with no source of truth to restore from).
  app.post("/config/tags/restore", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { tag?: string };
      const tag = typeof body.tag === "string" ? body.tag.trim().toLowerCase() : "";

      if(tag.length === 0) {

        res.status(400).json({ error: "Tag name is required.", success: false });

        return;
      }

      const registry = getTagRegistry();

      if(!registry.deletedTags.includes(tag)) {

        res.status(404).json({ error: "Tag '" + tag + "' is not a deleted predefined tag.", success: false });

        return;
      }

      registry.deletedTags = registry.deletedTags.filter((t) => t !== tag);
      setTagRegistry(registry);

      // Cascade-restore: add the tag back to predefined channels whose definition includes it but whose current resolved tags don't (stripped during cascade
      // delete). transformChannelTags handles loading, delta normalization, and persistence. The normalizer strips the tags delta when the result matches the
      // predefined definition, cleanly reverting the channel to its default state.
      const { affectedKeys, error } = await transformChannelTags(
        (entry) => {

          const predefined = getPredefinedChannel(entry.key);

          return (predefined?.tags?.includes(tag) === true) && (entry.channel.tags?.includes(tag) !== true);
        },
        (tags) => [ ...tags, tag ]
      );

      if(error) {

        res.status(400).json({ error, success: false });

        return;
      }

      LOG.info("Restored predefined tag '%s' on %d channels.", tag, affectedKeys.length);

      const profiles = getProfiles();

      res.json({

        active: getActiveTagVocabulary(),
        filterContent: generateTagFilterContent(), modalBody: generateTagManagerBody(),
        patch: (affectedKeys.length > 0) ? buildChannelTablePatch(affectedKeys, profiles) : undefined,
        registry: getTagRegistry(),
        success: true
      });
    } catch(error) {

      LOG.error("Failed to restore tag: %s.", formatError(error));
      res.status(500).json({ error: "Failed to restore tag: " + formatError(error), success: false });
    }
  });

  // POST /config/tags/rename - Rename a tag atomically across the vocabulary and all channel assignments. For predefined tags, the old name is added to
  // deletedTags and the new name is added to user tags. For user tags, the old name is replaced with the new name. All channels with the old tag get the
  // new tag substituted in their stored data (predefined channels via delta, user channels via direct modification).
  app.post("/config/tags/rename", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as { newTag?: string; oldTag?: string };
      const oldTag = typeof body.oldTag === "string" ? body.oldTag.trim().toLowerCase() : "";
      const newTag = typeof body.newTag === "string" ? body.newTag.trim().toLowerCase() : "";

      if(!oldTag || !newTag) {

        res.status(400).json({ error: "Both old and new tag names are required.", success: false });

        return;
      }

      if(oldTag === newTag) {

        res.status(400).json({ error: "New tag name must differ from the old name.", success: false });

        return;
      }

      if(!(/^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/).test(newTag)) {

        res.status(400).json({

          error: "Tag name must start and end with a letter or number, and contain only lowercase letters, numbers, and hyphens.", success: false
        });

        return;
      }

      if(newTag.length > 30) {

        res.status(400).json({ error: "Tag name must be 30 characters or less.", success: false });

        return;
      }

      // Validate old tag exists in the active vocabulary.
      const vocabulary = new Set(getActiveTagVocabulary());

      if(!vocabulary.has(oldTag)) {

        res.status(404).json({ error: "Tag '" + oldTag + "' not found.", success: false });

        return;
      }

      // Validate new tag doesn't already exist.
      if(vocabulary.has(newTag)) {

        res.status(409).json({ error: "Tag '" + newTag + "' already exists.", success: false });

        return;
      }

      // Update the tag registry: replace old with new in the appropriate list.
      const registry = getTagRegistry();
      const oldIsPredefined = PREDEFINED_TAGS.includes(oldTag);

      if(oldIsPredefined) {

        // Predefined tag: "delete" the old (add to deletedTags) and create the new as a user tag.
        if(!registry.deletedTags.includes(oldTag)) {

          registry.deletedTags.push(oldTag);
        }

        registry.tags.push(newTag);
      } else {

        // User tag: replace in the user tags list.
        registry.tags = registry.tags.map((t) => (t === oldTag) ? newTag : t);
      }

      setTagRegistry(registry);

      // Cascade: substitute the old tag with the new tag across all channel assignments. transformChannelTags handles loading, delta normalization, and persistence.
      const { affectedKeys, error } = await transformChannelTags(
        (entry) => entry.channel.tags?.includes(oldTag) === true,
        (tags) => tags.map((t) => (t === oldTag) ? newTag : t)
      );

      if(error) {

        res.status(400).json({ error, success: false });

        return;
      }

      LOG.info("Renamed tag '%s' to '%s' across %d channels.", oldTag, newTag, affectedKeys.length);

      const profiles = getProfiles();

      res.json({

        active: getActiveTagVocabulary(),
        filterContent: generateTagFilterContent(),
        modalBody: generateTagManagerBody(),
        patch: (affectedKeys.length > 0) ? buildChannelTablePatch(affectedKeys, profiles) : undefined,
        registry: getTagRegistry(),
        success: true
      });
    } catch(error) {

      LOG.error("Failed to rename tag: %s.", formatError(error));
      res.status(500).json({ error: "Failed to rename tag: " + formatError(error), success: false });
    }
  });
}
