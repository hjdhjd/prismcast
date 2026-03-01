/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * shared.ts: Shared utility types used across all PrismCast type domains.
 */

/**
 * A utility type that represents a value that can be null.
 * @typeParam T - The type that can be nullable.
 */
export type Nullable<T> = T | null;

// Sortable column field names for the channels table.
export type ChannelSortField = "channelNumber" | "channelSelector" | "key" | "name" | "profile" | "provider" | "stationId";

// Sort direction for the channels table.
export type SortDirection = "asc" | "desc";
