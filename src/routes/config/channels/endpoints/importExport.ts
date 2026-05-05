/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * importExport.ts: Channel import/export endpoints for JSON and M3U formats.
 *
 * Export produces a JSON snapshot of user channels with deltas resolved to full definitions (backward compatibility for the import validator that requires url).
 * Import JSON replaces all user channels atomically. Import M3U parses an M3U playlist, validates each entry, and applies a skip/replace conflict policy.
 */
import type { Channel, StoredChannelMap } from "../../../../types/index.ts";
import type { Express, Request, Response } from "express";
import { LOG, generateChannelKey, parseM3U, sanitizeString, stringifySorted } from "../../../../utils/index.ts";
import { type UserChannel, getChannelListing, getUserChannels, mutateChannels, resolveStoredChannel, validateChannelName, validateChannelUrl,
  validateImportedChannels } from "../../../../config/userChannels.ts";
import { sendSuccess, sendValidationError } from "../../http/envelope.ts";
import { buildChannelTablePatch } from "../table.ts";
import { getProfiles } from "../../../../config/profiles.ts";
import { route } from "../http/handler.ts";

type ConflictMode = "replace" | "skip";

/**
 * Registers the import/export endpoints on the Express application.
 * @param app - The Express application.
 */
export function registerImportExportRoutes(app: Express): void {

  // GET /config/channels/export - Export user channels as JSON. Deltas are resolved to full definitions for backward compatibility with the import validator which
  // requires url as a required field.
  app.get("/config/channels/export", route("export channels", (_req: Request, res: Response): void => {

    const storedChannels: StoredChannelMap = getUserChannels();
    const resolved: Record<string, Channel> = {};

    for(const [ key, stored ] of Object.entries(storedChannels)) {

      resolved[key] = resolveStoredChannel(key, stored);
    }

    res.setHeader("Content-Type", "application/json");
    res.setHeader("Content-Disposition", "attachment; filename=\"prismcast-channels.json\"");
    res.send(stringifySorted(resolved) + "\n");
  }));

  // POST /config/channels/import - Import channels from JSON, replacing all existing user channels.
  app.post("/config/channels/import", route("import channels", async (req: Request, res: Response) => {

    const rawData: unknown = req.body;
    const validProfiles = getProfiles().map((p) => p.name);
    const validationResult = validateImportedChannels(rawData, validProfiles);

    if(!validationResult.valid) {

      sendValidationError(res, "Validation errors:\n" + validationResult.errors.join("\n"));

      return;
    }

    // Replace the channels map outright. Reassigning preserves the surrounding metadata fields (migrationsApplied, schemaVersion, serviceSelections,
    // tagRegistry) - mutating those was the original defect that broke imports at runtime by leaving data.channels undefined for the framework's post-mutate
    // normalize step.
    await mutateChannels((data) => {

      data.channels = { ...validationResult.channels };
    });

    const channelCount = Object.keys(validationResult.channels).length;

    // A bulk JSON import replaces every user channel - return a patch covering every key in the listing.
    const allKeys = getChannelListing().map((entry) => entry.key);

    const message = "Imported " + String(channelCount) + " channel" + (channelCount === 1 ? "" : "s") + " successfully.";

    sendSuccess(res, { affectedKeys: allKeys, message });
  }));

  // POST /config/channels/import-m3u - Import channels from an M3U playlist file.
  app.post("/config/channels/import-m3u", route("import channels", async (req: Request, res: Response) => {

    const body = req.body as { conflictMode?: string; content?: string };
    const content = body.content;
    const conflictMode: ConflictMode = (body.conflictMode === "replace") ? "replace" : "skip";

    if(!content || (typeof content !== "string") || (content.trim() === "")) {

      sendValidationError(res, "No M3U content provided.");

      return;
    }

    if((body.conflictMode !== undefined) && (body.conflictMode !== "skip") && (body.conflictMode !== "replace")) {

      sendValidationError(res, "Invalid conflict mode. Must be 'skip' or 'replace'.");

      return;
    }

    const parseResult = parseM3U(content);

    if(parseResult.channels.length === 0) {

      sendValidationError(res, "No channels found in M3U file." + (parseResult.errors.length > 0 ? " Parse errors: " + parseResult.errors.join("; ") : ""));

      return;
    }

    const affectedKeys: string[] = [];
    const conflicts: string[] = [];
    const importErrors: string[] = [];
    const seenKeys = new Set<string>();
    let imported = 0;
    let replaced = 0;
    let skipped = 0;

    // Pre-validate and sanitize each parsed channel before entering the mutation. This keeps pure validation outside the serialized write lock.
    const validEntries: { channel: UserChannel; key: string }[] = [];

    for(const m3uChannel of parseResult.channels) {

      m3uChannel.name = sanitizeString(m3uChannel.name);
      m3uChannel.url = sanitizeString(m3uChannel.url);
      m3uChannel.stationId &&= sanitizeString(m3uChannel.stationId);

      const key = generateChannelKey(m3uChannel.name);

      if(!key || (key.length === 0)) {

        importErrors.push("Could not generate key for channel '" + m3uChannel.name + "'.");

        continue;
      }

      // Skip duplicate keys within the same M3U file (first occurrence wins).
      if(seenKeys.has(key)) {

        continue;
      }

      seenKeys.add(key);

      const urlError = validateChannelUrl(m3uChannel.url);

      if(urlError) {

        importErrors.push("Channel '" + m3uChannel.name + "': " + urlError);

        continue;
      }

      const nameError = validateChannelName(m3uChannel.name);

      if(nameError) {

        importErrors.push("Channel '" + m3uChannel.name + "': " + nameError);

        continue;
      }

      const channel: UserChannel = {

        name: m3uChannel.name,
        url: m3uChannel.url
      };

      if(m3uChannel.stationId) {

        channel.stationId = m3uChannel.stationId;
      }

      validEntries.push({ channel, key });
    }

    // Apply the validated entries inside the transactional mutation. Conflict detection happens against the current stored channels.
    await mutateChannels((data) => {

      for(const { channel, key } of validEntries) {

        if(key in data.channels) {

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

        data.channels[key] = channel;
        affectedKeys.push(key);
      }
    });

    LOG.info("M3U import completed: %d imported, %d replaced, %d skipped.", imported, replaced, skipped);

    const patch = ((imported > 0) || (replaced > 0)) ? buildChannelTablePatch(affectedKeys, getProfiles()) : undefined;

    sendSuccess(res, {

      data: { conflicts, errors: [ ...parseResult.errors, ...importErrors ], imported, replaced, skipped },
      patch
    });
  }));
}
