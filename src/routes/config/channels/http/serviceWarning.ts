/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * serviceWarning.ts: Shared helper for building the "service not in active filter" warning.
 *
 * When a user adds a channel whose service tag isn't in the active service filter, the channel would be hidden from the playlist and table immediately. The
 * warning payload tells the client to show a toast with a one-click enable action. Both the browse endpoint (bulk add) and the CRUD endpoint (single add) use this
 * helper so the warning policy is defined exactly once.
 */
import { getEnabledServices, getServiceDisplayName, isServiceTagEnabled } from "../../../../config/services.ts";
import { getDomainConfig } from "../../../../config/sites.ts";

/**
 * Builds a service filter warning payload when a URL's service tag is not in the active filter. Returns undefined when no filter is active, the URL resolves to
 * the "direct" (non-service) tag, or the tag is already enabled.
 * @param url - The channel URL to derive the service tag from.
 * @returns The warning with display label and tag, or undefined when no warning is needed.
 */
export function buildServiceFilterWarning(url: string): { serviceLabel: string; serviceTag: string } | undefined {

  // No active filter means every service is visible - no warning needed.
  if(getEnabledServices().length === 0) {

    return undefined;
  }

  const tag = getDomainConfig(url)?.serviceTag;

  if(tag && (tag !== "direct") && !isServiceTagEnabled(tag)) {

    return { serviceLabel: getServiceDisplayName(url), serviceTag: tag };
  }

  return undefined;
}
