/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Configuration endpoint coordinator for PrismCast.
 */
import { LOG, isRunningAsService } from "../../utils/index.ts";
import type { Express } from "express";
import type { ProfileCategory } from "../../types/index.ts";
import type { ProfileInfo } from "../../config/profiles.ts";
import { closeBrowser } from "../../browser/index.ts";
import { getStreamCount } from "../../streaming/registry.ts";
import { setupChannelRoutes } from "./channels/index.ts";
import { setupProfileRoutes } from "./services.ts";
import { setupSettingsRoutes } from "./settings.ts";

/**
 * Result of scheduling a server restart.
 */
export interface RestartResult {

  // Number of active streams at the time of the restart request.
  activeStreams: number;

  // Whether the restart was deferred due to active streams.
  deferred: boolean;

  // The message to display to the user.
  message: string;

  // Whether the server will auto-restart (true if running as a service, false if manual restart required).
  willRestart: boolean;
}

/**
 * Schedules a server restart after a brief delay to allow the response to be sent. This is used after configuration changes that require a restart to take effect.
 * Returns information about whether the server will auto-restart (depends on whether running as a service). If streams are active and running as a service, the restart
 * is deferred until streams end, allowing the client to show a dialog and let the user choose to wait or force restart.
 * @param reason - A description of why the server is restarting, used in the log message.
 * @returns Information about the restart including the message to display and whether auto-restart will occur.
 */
export function scheduleServerRestart(reason: string): RestartResult {

  const willRestart = isRunningAsService();

  // When not running as a service, we can't auto-restart. Notify the user that a manual restart is required.
  if(!willRestart) {

    LOG.info("Configuration saved %s. Manual restart required for changes to take effect.", reason);

    return {

      activeStreams: 0,
      deferred: false,
      message: "Configuration saved. Please restart PrismCast for changes to take effect.",
      willRestart: false
    };
  }

  // Check for active streams. If streams are active, defer the restart to avoid interrupting recordings or live viewing.
  const activeStreams = getStreamCount();

  if(activeStreams > 0) {

    LOG.info("Configuration saved %s. Restart deferred until %d active stream(s) end.", reason, activeStreams);

    return {

      activeStreams,
      deferred: true,
      message: "Configuration saved. " + String(activeStreams) + " stream(s) are active.",
      willRestart: true
    };
  }

  // No active streams - restart immediately. Close the browser first to avoid orphan Chrome processes.
  setTimeout(() => {

    LOG.info("Exiting for service manager restart %s.", reason);

    void closeBrowser().then(() => { process.exit(0); }).catch(() => { process.exit(1); });
  }, 500);

  return {

    activeStreams: 0,
    deferred: false,
    message: "Configuration saved. Server is restarting...",
    willRestart: true
  };
}

/**
 * Groups profiles by their declared category for UI display. Each profile declares its own category (api, keyboard, multiChannel, special) and this helper
 * simply filters by that field. The display order (api, keyboard, special, multiChannel) is determined by the caller.
 * @param profiles - List of available profiles with category, descriptions, and summaries.
 * @returns Object with profiles grouped by category.
 */
export function categorizeProfiles(profiles: readonly ProfileInfo[]): Record<ProfileCategory, ProfileInfo[]> {

  return {

    api: profiles.filter((p) => (p.category === "api")),
    custom: profiles.filter((p) => (p.category === "custom")),
    keyboard: profiles.filter((p) => (p.category === "keyboard")),
    multiChannel: profiles.filter((p) => (p.category === "multiChannel")),
    special: profiles.filter((p) => (p.category === "special"))
  };
}

/**
 * Configures the configuration endpoints. The configuration UI is rendered on the main page and accessed via hash navigation (/#config/<section>, /#channels);
 * this function mounts the data endpoints under /config that the client-side scripts call - settings, channels, and profiles routes.
 * @param app - The Express application.
 */
export function setupConfigEndpoint(app: Express): void {

  setupSettingsRoutes(app);
  setupChannelRoutes(app);
  setupProfileRoutes(app);
}

// Barrel re-exports for external consumers.

export type { ChannelRowHtml } from "./channels/index.ts";
export { OPTIONAL_COLUMNS, generateChannelRowHtml, generateChannelsPanel, generateServiceFilterToolbar } from "./channels/index.ts";
export { generateAdvancedTabContent, generateCollapsibleSection, generateSettingsFormFooter, generateSettingsTabContent,
  hasEnvOverrides } from "./settings.ts";
export { generateCustomProfilesPanel, generateProfileWizardModal } from "./services.ts";
