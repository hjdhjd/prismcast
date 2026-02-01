/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channels.ts: Channel listing route for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { getChannelListing } from "../config/userChannels.js";

/*
 * CHANNEL LISTING
 *
 * The channels endpoint provides a JSON representation of all available channels (predefined + user-defined). This gives programmatic access to the full channel
 * list with source and enabled metadata, complementing the M3U playlist endpoint which only includes enabled channels in playlist format.
 */

/**
 * Response entry for a single channel in the GET /channels response.
 */
interface ChannelEntry {

  // Numeric channel number for HDHomeRun/Plex guide matching.
  channelNumber?: number;

  // CSS selector for channel selection within a multi-channel player.
  channelSelector?: string;

  // Whether the channel is enabled for streaming and playlist inclusion.
  enabled: boolean;

  // The channel key (URL-safe slug used in stream URLs).
  key: string;

  // Human-readable display name.
  name: string;

  // Profile name override for site-specific behavior.
  profile?: string;

  // Where this channel comes from: "predefined" (built-in), "user" (user-defined), or "override" (user channel replacing a predefined one).
  source: "override" | "predefined" | "user";

  // Gracenote station ID for electronic program guide integration.
  stationId?: string;

  // URL of the streaming page to capture.
  url: string;
}

/**
 * Creates an endpoint to list all channels with their source and enabled status.
 * @param app - The Express application.
 */
export function setupChannelsEndpoint(app: Express): void {

  app.get("/channels", (_req: Request, res: Response): void => {

    const listing = getChannelListing();
    const channels: ChannelEntry[] = [];

    for(const entry of listing) {

      // Build the response entry with required fields.
      const channelEntry: ChannelEntry = {

        enabled: entry.enabled,
        key: entry.key,
        name: entry.channel.name,
        source: entry.source,
        url: entry.channel.url
      };

      // Add optional fields only when present.
      if(entry.channel.channelNumber !== undefined) {

        channelEntry.channelNumber = entry.channel.channelNumber;
      }

      if(entry.channel.channelSelector) {

        channelEntry.channelSelector = entry.channel.channelSelector;
      }

      if(entry.channel.profile) {

        channelEntry.profile = entry.channel.profile;
      }

      if(entry.channel.stationId) {

        channelEntry.stationId = entry.channel.stationId;
      }

      channels.push(channelEntry);
    }

    res.json({

      channels,
      count: channels.length
    });
  });
}
