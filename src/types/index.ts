/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Barrel re-export for PrismCast type definitions.
 */
export type { BrowserConfig, CaptureMode, ChannelsConfig, Config, HdhrConfig, HLSConfig, LoggingConfig, PathsConfig, PlaybackConfig, RecoveryConfig,
  ServerConfig, StreamingConfig } from "./config.ts";
export type { CanonicalChannel, Channel, ChannelDefinition, ChannelDelta, ChannelIdentity, ChannelListingEntry, ChannelMap, ChannelServiceBinding,
  CustomizableField, ResolvedChannel, ResolvedChannelMap, ServiceGroup, ServiceVariant, StoredChannel, StoredChannelMap, VariantChannel } from "./channels.ts";
export { CHANNEL_BINDING_KEYS, CHANNEL_IDENTITY_KEYS, DELTA_ELIGIBLE_BINDING_KEYS, DELTA_ELIGIBLE_IDENTITY_KEYS } from "./channels.ts";
export type { ChannelSelectionConfig, ChannelSelectionStrategy, DomainConfig, ProfileCategory, ProfileCategoryInfo, ProfileResolutionResult,
  ProfilesValidationResult, ResolvedSiteProfile, ServicePack, SiteProfile, UserProfilesFile, UserProfilesLoadResult } from "./profiles.ts";
export { PROFILE_CATEGORIES } from "./profiles.ts";
export type { AuthWallIndicators, CategoryResolution, CategoryResolutionConfig, CategoryResolutionFailure, CategoryResolutionSuccess, ChannelSelectionProfile,
  ChannelSelectorResult, ChannelStrategyEntry, ChannelStrategyHandler, ClickTarget, DiscoveredChannel, ProviderModule, TuneResult } from "./selection.ts";
export type { ChannelSortField, Nullable, SortDirection } from "./shared.ts";
export type { CaptureCodec, HealthStatus, MediaContainer, StreamListItem, StreamListResponse, StreamingMode, UrlValidationResult, VideoSelectorType,
  VideoState } from "./streaming.ts";
export { RECOGNIZED_CODECS } from "./streaming.ts";
export { isChannelSelectionProfile } from "./selection.ts";
