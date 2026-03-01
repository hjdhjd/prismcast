/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Barrel re-export for PrismCast type definitions.
 */
export type { BrowserConfig, CaptureMode, ChannelsConfig, Config, HdhrConfig, HLSConfig, LoggingConfig, PathsConfig, PlaybackConfig, RecoveryConfig,
  ServerConfig, StreamingConfig } from "./config.js";
export type { Channel, ChannelDelta, ChannelListingEntry, ChannelMap, ProviderGroup, StoredChannel, StoredChannelMap } from "./channels.js";
export type { ChannelSelectionConfig, ChannelSelectionStrategy, DomainConfig, ProfileCategory, ProfileResolutionResult, ProfilesValidationResult, ProviderPack,
  ResolvedSiteProfile, SiteProfile, UserProfilesFile, UserProfilesLoadResult } from "./profiles.js";
export type { ChannelSelectionProfile, ChannelSelectorResult, ChannelStrategyEntry, ChannelStrategyHandler, ClickTarget, DiscoveredChannel, ProviderModule,
  TuneResult, UiSize } from "./selection.js";
export type { ChannelSortField, Nullable, SortDirection } from "./shared.js";
export type { HealthStatus, StreamInfo, StreamListItem, StreamListResponse, UrlValidation, UrlValidationResult, VideoSelectorType,
  VideoState } from "./streaming.js";
export { isChannelSelectionProfile } from "./selection.js";
