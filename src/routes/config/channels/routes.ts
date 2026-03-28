/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * routes.ts: Channel route handlers for the PrismCast configuration interface.
 */
import type { Channel, ChannelDelta, ChannelSortField, Nullable, StoredChannel } from "../../../types/index.js";
import type { Express, Request, Response } from "express";
import { LOG, formatError, generateChannelKey, parseM3U, sanitizeString, stringifySorted } from "../../../utils/index.js";
import { VALID_OPTIONAL_COLUMNS, buildChannelTablePatch, buildChannelTableState } from "./table.js";
import { VALID_SORT_FIELDS, compareChannelSort, getAllProviderTags, getCanonicalKey, getChannelProviderLabel, getEnabledProviders, getProviderGroup,
  getProviderSelection, getProviderTagForChannel, getResolvedChannel, resolvePredefinedVariant, resolveProviderKey, setEnabledProviders,
  setProviderSelection } from "../../../config/providers.js";
import { filterDefaults, loadUserConfig, saveUserConfig } from "../../../config/userConfig.js";
import { getChannelListing, getEastWithPacificPredefinedKeys, getPacificPredefinedKeys, getPredefinedChannel, getPredefinedChannels,
  getUserChannels, isPredefinedChannel, isUserChannel, loadUserChannels, resolveStoredChannel,
  saveProviderSelections, saveUserChannels, validateChannelKey, validateChannelName, validateChannelNumber, validateChannelProfile, validateChannelUrl,
  validateImportedChannels } from "../../../config/userChannels.js";
import { CONFIG } from "../../../config/index.js";
import { PREDEFINED_CHANNELS } from "../../../channels/index.js";
import type { UserChannel } from "../../../config/userChannels.js";
import { getProfiles } from "../../../config/profiles.js";
import { updateChannelLogo } from "../../../streaming/showInfo.js";

// Fields that appear in the generated M3U playlist and affect Channels DVR's view of the channel. Used to decide whether the playlist reload hint is shown.
const M3U_FIELDS = [ "channelNumber", "name", "stationId", "tvgShift" ];

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
  // re-enables a disabled predefined channel and sets its provider, 'switch' changes the provider selection for an existing channel, and 'remove' reverts a
  // channel to its canonical provider (disabling it if no alternative provider is available). Channel writes and provider selection changes are batched into
  // single saves to avoid redundant file I/O.
  app.post("/config/channels/modify", async (req: Request, res: Response): Promise<void> => {

    try {

      const body = req.body as {
        channels?: {
          action?: string; canonicalKey?: string; channelSelector?: string; name?: string; providerSlug?: string; stationId?: string; url?: string;
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

      // Build a UserChannel object from an entry's fields. Shared by the switch/enable and add paths to avoid duplicating channel construction logic.
      function buildUserChannel(entry: { channelSelector?: string; name?: string; stationId?: string; url?: string },
        channelName: string, channelUrl: string, selector: string): UserChannel {

        const channel: UserChannel = {

          channelSelector: selector || undefined,
          name: channelName,
          url: channelUrl
        };

        if(entry.stationId) {

          channel.stationId = sanitizeString(entry.stationId);
        }

        return channel;
      }

      for(const entry of channels) {

        const action = entry.action ?? "add";
        const name = sanitizeString(entry.name?.trim() ?? "");
        const providerSlug = entry.providerSlug?.trim() ?? "";

        // Handle enable and switch. Enable re-enables a disabled predefined channel, then falls through to the switch logic to set the provider selection.
        // Switch changes the provider selection for an existing channel to point to the browsed provider's variant, creating the variant if needed.
        if((action === "switch") || (action === "enable")) {

          const canonicalKey = entry.canonicalKey?.trim() ?? "";

          if(!canonicalKey) {

            errors.push("Entry for '" + name + "' missing canonical key.");

            continue;
          }

          // Re-enable the channel if it was disabled. Must complete before setting the provider selection since the config write makes the channel
          // visible in the lineup.
          if(action === "enable") {

            // eslint-disable-next-line no-await-in-loop -- Sequential: config state must be updated before setting provider selection.
            await enablePredefinedChannels([canonicalKey]);
          }

          const variantKey = canonicalKey + "-" + providerSlug;

          // If no variant exists for this provider, create one as a user channel.
          if(!allKeys.has(variantKey)) {

            existingChannels[variantKey] = buildUserChannel(entry, name, sanitizeString(entry.url?.trim() ?? ""),
              sanitizeString(entry.channelSelector?.trim() ?? ""));
            allKeys.add(variantKey);
            channelsChanged = true;
          }

          setProviderSelection(canonicalKey, variantKey);
          selectionsChanged = true;
          switched++;
          affectedKeys.add(canonicalKey);

          continue;
        }

        // Handle provider removal. Reverts the channel away from this provider using a three-tier fallback: (1) clear the selection and let
        // resolveProviderKey find the next enabled variant or canonical, (2) if the resolved provider is still this provider (no alternative exists),
        // disable the predefined channel or delete the user channel.
        if(action === "remove") {

          const canonicalKey = entry.canonicalKey?.trim() ?? "";

          if(!canonicalKey) {

            errors.push("Remove entry for '" + name + "' missing canonical key.");

            continue;
          }

          // Clear the provider selection. resolveProviderKey will now return the canonical or the first enabled alternative variant.
          setProviderSelection(canonicalKey, canonicalKey);
          selectionsChanged = true;

          // Check if the resolved provider is still this provider. If so, there is no alternative — disable or delete the channel.
          const resolvedKey = resolveProviderKey(canonicalKey);
          const resolvedTag = getProviderTagForChannel(resolvedKey);

          if(resolvedTag === providerSlug) {

            // No alternative provider exists. Disable predefined channels or delete user channels.
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
        // existing canonical). Channels that match existing canonicals appear as 'switch' state in the modal instead.
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
        const key = (canonicalExists && providerSlug) ? baseKey + "-" + providerSlug : baseKey;

        // Skip channels that already exist in the lineup.
        if(allKeys.has(key)) {

          continue;
        }

        const newChannel = buildUserChannel(entry, name, url, channelSelector);

        // When this is a provider variant (key differs from baseKey), store the canonical relationship explicitly so that buildProviderGroups can
        // associate the user channel with its canonical via the canonicalKey field.
        if(key !== baseKey) {

          newChannel.canonicalKey = baseKey;
        }

        existingChannels[key] = newChannel;
        allKeys.add(key);
        channelsChanged = true;
        added++;
        affectedKeys.add(canonicalExists ? baseKey : key);
      }

      // Save changes. saveUserChannels() persists both channel data and provider selections in a single file write, so we only need saveProviderSelections()
      // (which writes the same file using the module-level cache) when selections changed but no new channels were added.
      if(channelsChanged) {

        await saveUserChannels(existingChannels);
      } else if(selectionsChanged) {

        await saveProviderSelections();
      }

      // Disable predefined channels that had no alternative provider after removal.
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

      res.json({ added, errors, message, patch: buildChannelTablePatch([...affectedKeys], getProfiles()), removed, success: true, switched });
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

      // Resolve display names for logging before generating the row HTML.
      const canonicalChannel = getResolvedChannel(canonicalKey);
      const variantChannel = getResolvedChannel(providerKey);
      const channelName = canonicalChannel?.name ?? canonicalKey;
      const providerLabel = variantChannel ? getChannelProviderLabel(variantChannel) : providerKey;

      LOG.info("Provider for %s changed to %s.", channelName, providerLabel);

      // Return a channel table patch so the client can update the row and summary counts in place.
      const profiles = getProfiles();

      res.json({ channel: canonicalKey, patch: buildChannelTablePatch([canonicalKey], profiles), provider: providerKey, success: true });
    } catch(error) {

      LOG.error("Failed to update provider selection: %s.", formatError(error));
      res.status(500).json({ error: "Failed to update provider: " + formatError(error), success: false });
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
  // start is 0. Overwrites existing channel numbers for affected channels. Only channels that are enabled and available by provider filter are affected.
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

      // Get visible channels (enabled + available by provider filter) sorted by the user's current sort order.
      const listing = getChannelListing().filter((entry) => entry.enabled && entry.availableByProvider);

      listing.sort((a, b) => compareChannelSort(a.channel, a.key, b.channel, b.key, sortField, sortDir));

      // Load user channels for modification.
      const result = await loadUserChannels();

      if(result.parseError) {

        res.status(400).json({ error: "Cannot auto-number: channels file contains invalid JSON.", success: false });

        return;
      }

      const affectedKeys: string[] = [];

      if(clearMode) {

        // Clear channel numbers from all visible channels. For predefined channels, use null to signal removal in the delta. For user channels, use undefined.
        for(const entry of listing) {

          const existing = result.channels[entry.key] ?? {};
          const clearValue = isPredefinedChannel(entry.key) ? null : undefined;

          existing.channelNumber = clearValue;
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

      // Get visible channels (enabled + available by provider filter).
      const listing = getChannelListing().filter((entry) => entry.enabled && entry.availableByProvider);

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
        const clearValue = isPredefinedChannel(entry.key) ? null : undefined;

        existing.hdhrEnabled = enable ? clearValue : false;
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

      // Validate all tags are known. Tags already in enabledProviders are accepted even if no current channel or profile produces them — this allows stale tags to be
      // removed via the UI without blocking the request.
      const knownTags = new Set(getAllProviderTags().map((t) => t.tag));
      const currentTags = new Set(getEnabledProviders());

      for(const tag of tags) {

        if(!knownTags.has(tag) && !currentTags.has(tag)) {

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

      // Return counts patch so the client can update summary counts after filter change. No rows — the client applies CSS visibility changes itself.
      const { counts, scopeCounts } = buildChannelTableState();

      res.json({ enabledProviders: tags, patch: { counts, rows: [], scopeCounts }, success: true });
    } catch(error) {

      LOG.error("Failed to update provider filter: %s.", formatError(error));
      res.status(500).json({ error: "Failed to update provider filter: " + formatError(error), success: false });
    }
  });

  // POST /config/channels/setup-completed - Mark the Provider Setup flow as completed. Called when the wizard finishes or the user explicitly skips.
  app.post("/config/channels/setup-completed", async (_req: Request, res: Response): Promise<void> => {

    try {

      CONFIG.channels.setupCompleted = true;

      const configResult = await loadUserConfig();

      configResult.config.channels ??= {};
      configResult.config.channels.setupCompleted = true;

      await saveUserConfig(configResult.config);

      // Return a full patch so the client can refresh the channel table after the setup wizard completes. The wizard's browse step may have added channels
      // via the modify endpoint, and the provider filter may have changed — the patch ensures the table reflects the current state.
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

      LOG.error("Failed to bulk assign provider: %s.", formatError(error));
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

      LOG.error("Failed to bulk restore providers: %s.", formatError(error));
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

        if((field !== "channelNumber") && (field !== "hdhrEnabled") && (field !== "stationId")) {

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

        // Use a StoredChannelMap-compatible update. The ?? {} creates an empty ChannelDelta for predefined channels that don't have a user override yet.
        const stored = result.channels[key] ?? {};
        const clearValue = isPredefinedChannel(key) ? null : undefined;

        if(field === "channelNumber") {

          stored.channelNumber = value ? parseInt(value, 10) : clearValue;
        } else if(field === "hdhrEnabled") {

          // hdhrEnabled: "true" means enabled (clear the override), "false" means disabled. For predefined channels, null clears the delta. For user channels,
          // delete the field entirely to revert to the default (included).
          stored.hdhrEnabled = (value === "false") ? false : clearValue;
        } else {

          stored.stationId = value || clearValue;
        }

        result.channels[key] = stored;

        await saveUserChannels(result.channels);

        const fieldLabels: Record<string, string> = { channelNumber: "Channel number", hdhrEnabled: "HDHR lineup", stationId: "Station ID" };
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

            (delta as Record<string, boolean | string | number | null | undefined>)[field] = ((formVal === "") || (formVal === undefined)) ? null : formVal;
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

            res.json({ key, message: "Channel '" + key + "' reverted to defaults." + implicitRevertHint,
              patch: buildChannelTablePatch([key], profiles), success: true });

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

          res.json({ key, message: "Channel '" + key + "' reverted to defaults." + variantRevertHint,
            patch: buildChannelTablePatch([key], profiles), success: true });

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

      // Return success response with patch for client-side DOM update. Changes take effect immediately due to hot-reloading in saveUserChannels().
      res.json({

        isNew: action === "add",
        key,
        message: "Channel '" + key + "' " + actionLabel + " successfully." + playlistHint,
        patch: buildChannelTablePatch([key], profiles),
        success: true
      });
    } catch(error) {

      LOG.error("Failed to save channel: %s.", formatError(error));
      res.status(500).json({ message: "Failed to save channel: " + formatError(error), success: false });
    }
  });
}
