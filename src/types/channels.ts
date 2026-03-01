/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * channels.ts: Channel definition and channel map type definitions for PrismCast.
 */
import type { Nullable } from "./shared.js";

/* Channels map short URL-friendly names to streaming site URLs with optional metadata. The channel name appears in stream URLs (e.g., /stream/nbc) and must be
 * URL-safe. Channel definitions can override profile settings for specific channels and provide metadata for M3U playlist generation.
 */

/**
 * Channel definition mapping a short name to a streaming URL with optional configuration overrides.
 */
export interface Channel {

  // Numeric channel number for guide matching. When set, this number is used as the channel-number in the M3U playlist for Channels DVR and as the GuideNumber in
  // the HDHomeRun lineup for Plex. When omitted, no channel number is included in the M3U playlist and a number is auto-assigned for HDHomeRun.
  channelNumber?: number;

  // CSS selector for channel selection within a multi-channel player. This overrides any channelSelector in the profile. Used for sites like Pluto TV where the
  // base URL is the same but different channels require clicking different UI elements.
  channelSelector?: string;

  // Human-readable channel name displayed in the M3U playlist. This is what users see in their channel guide. Use proper capitalization and include network
  // suffixes like "HD" or regional identifiers like "(Pacific)" where appropriate. Required for canonical channels; variants inherit from their canonical entry.
  name?: string;

  // Gracenote station ID for the Pacific timezone feed. When present on an East canonical entry, the system auto-generates a Pacific canonical ("{key}p") and
  // matching Pacific provider variants at startup. The generated Pacific entry inherits name, url, and channelSelector from this entry. See the
  // generatePacificEntries() function in channels/index.ts for the full generation rules, examples, and instructions for adding new channels.
  pacificStationId?: string;

  // Profile name to use for this channel, overriding URL-based profile detection. Use this when a site's behavior doesn't match what would be inferred from its
  // domain, or when a specific channel needs different handling than others on the same site.
  profile?: string;

  // Display name override for the provider selection dropdown. Normally auto-derived from the URL domain via DOMAIN_CONFIG in config/profiles.ts (e.g., a
  // hulu.com URL automatically resolves to "Hulu"). Only needed when a channel's display name should differ from the domain-level default.
  provider?: string;

  // CSS selector to narrow the DOM search when scrollTarget is set. Overrides the profile-level scrollSelector for this channel. See ChannelSelectionConfig for
  // full documentation.
  scrollSelector?: string;

  // Text content to match when scrolling a lazy-loaded section into view before channel selection. Overrides the profile-level scrollTarget for this channel. See
  // ChannelSelectionConfig for full documentation.
  scrollTarget?: string;

  // Whether to scroll to the bottom of the page before channel selection. Overrides the profile-level scrollToBottom for this channel. See
  // ChannelSelectionConfig for full documentation.
  scrollToBottom?: boolean;

  // Gracenote station ID for electronic program guide integration. When set, this ID is included in the M3U playlist as the tvc-guide-stationid attribute,
  // allowing Channels DVR to fetch program guide data for the channel.
  stationId?: string;

  // EPG time shift in hours. When set, this value is included in the M3U playlist as the tvg-shift attribute, telling Channels DVR to offset the guide data by
  // this many hours. Useful for time-delayed feeds that share a station ID with the primary feed (e.g., Pacific feeds that air 3 hours after the East feed).
  tvgShift?: number;

  // URL of the streaming page to capture. This should be the direct URL to the live stream player, not a landing page or show page. Authentication cookies from
  // the Chrome profile are used, so the URL can be to authenticated content.
  url: string;
}

/**
 * Enriched channel entry returned by getChannelListing(). Wraps a Channel definition with source classification and enabled status metadata, providing the
 * single source of truth for merged channel data across the codebase.
 */
export interface ChannelListingEntry {

  // Whether the channel has at least one provider variant available given the current provider filter. When false, the channel is hidden from the playlist and guide.
  availableByProvider: boolean;

  // The channel definition with all properties (name, url, profile, etc.).
  channel: Channel;

  // Whether the channel is enabled for streaming and playlist inclusion. Disabled predefined channels (without user overrides) have this set to false.
  enabled: boolean;

  // The channel key (URL-safe slug used in stream URLs).
  key: string;

  // Where this channel comes from: "predefined" (built-in), "user" (user-defined), or "override" (user channel replacing a predefined one).
  source: "override" | "predefined" | "user";
}

/**
 * A delta override for a predefined channel. All fields are optional because only fields that differ from the predefined definition are stored. String and number
 * fields use Nullable<T> to distinguish "user cleared this field" (null) from "inherit from predefined" (absent). When a field is null, the predefined value is
 * removed in the resolved channel. When a field is absent, the predefined value is inherited.
 */
export interface ChannelDelta {

  // Override for channel number, or null to clear the predefined value.
  channelNumber?: Nullable<number>;

  // Override for channel selector, or null to clear the predefined value.
  channelSelector?: Nullable<string>;

  // Override for display name, or null to clear the predefined value.
  name?: Nullable<string>;

  // Override for profile, or null to clear the predefined value.
  profile?: Nullable<string>;

  // Override for station ID, or null to clear the predefined value.
  stationId?: Nullable<string>;

  // Override for EPG time shift, or null to clear the predefined value.
  tvgShift?: Nullable<number>;

  // Override for URL, or null to clear the predefined value. When absent, the predefined URL is inherited.
  url?: Nullable<string>;
}

/**
 * What gets stored in channels.json per key. For user-defined channels (no predefined equivalent), this is a full Channel with a required url. For overrides of
 * predefined channels, this can be a ChannelDelta with only the differing fields. Legacy full-override entries (from before the delta model) are also valid — they
 * are just deltas that happen to override every field.
 */
export type StoredChannel = Channel | ChannelDelta;

/**
 * Map of channel keys to stored channel data (full definitions or deltas). This is the raw type for the channels.json file contents.
 */
export type StoredChannelMap = Record<string, StoredChannel>;

/**
 * Map of channel short names to channel definitions. Channel names must be URL-safe strings (lowercase letters, numbers, hyphens) since they appear in stream
 * request URLs.
 */
export type ChannelMap = Record<string, Channel>;

/* Provider groups allow multiple streaming providers to offer the same content (e.g., ESPN via ESPN.com or Disney+). Channels are grouped by key prefix convention:
 * a key like "espn-disneyplus" is a variant of "espn" because it starts with "espn-" and "espn" exists as a channel. The canonical key (the base key without
 * suffix) is the default provider.
 */

/**
 * Represents a group of provider variants for the same content. Used by the UI to display provider selection dropdowns for multi-provider channels.
 */
export interface ProviderGroup {

  // The canonical channel key (without suffix), which is the default provider. Example: "espn" for the ESPN channel group.
  canonicalKey: string;

  // List of all provider variants including the canonical entry. Each variant has a key (channel key) and a display label for the UI.
  variants: {

    // Channel key for this provider variant. Example: "espn" or "espn-disneyplus".
    key: string;

    // UI display label derived from channel.provider (if set) or auto-resolved from the URL domain via getProviderDisplayName(). Example: "ESPN.com" or "Disney+".
    label: string;
  }[];
}
