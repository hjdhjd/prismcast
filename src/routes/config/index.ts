/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Configuration endpoint coordinator for PrismCast.
 */
import { LOG, isRunningAsService } from "../../utils/index.ts";
import type { Nullable, ProfileCategory } from "../../types/index.ts";
import type { ApplyResult } from "../../config/reactivity.ts";
import type { Express } from "express";
import type { ProfileInfo } from "../../config/profiles.ts";
import { closeBrowser } from "../../browser/index.ts";
import { getStreamCount } from "../../streaming/registry.ts";
import { reloadConfiguration } from "../../config/index.ts";
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
 * Combined result of applying a configuration change. apply describes which subsystems took the change live, deferred it, or rejected it; restart is non-null
 * only when at least one change deferred and a restart was scheduled. Callers use this shape to build the user-facing response message and to decide which UI
 * dialog to show (active-streams deferral, restart-in-progress spinner, or a simple toast).
 */
export interface ApplyConfigurationResult {

  // The result of dispatching the diff to registered handlers.
  apply: ApplyResult;

  // The restart schedule outcome, or null if no restart was scheduled.
  restart: Nullable<RestartResult>;
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
 * Reloads the in-memory configuration from disk, dispatches the diff to registered subsystem handlers, and schedules a server restart only if there are
 * changes that no handler could apply live. The single entry point both the /config save handler and /config/import handler call after writing to disk. The
 * returned shape lets each handler tailor its response message and pick between the "show toast" and "restart in progress" UI flows.
 *
 * Rejected changes do not trigger a restart on their own - rejection means a handler refused the change after the disk write, so the value is persisted but
 * the live side-effect did not occur (e.g., a handler that refused to start a port-conflicting server). Callers should surface rejected reasons to the user so
 * they can fix the underlying cause and re-save rather than restarting blindly.
 * @param reason - A description of why configuration is changing, used in the restart log message when a restart is scheduled.
 * @returns Combined apply and restart result.
 */
export async function applyConfigurationChange(reason: string): Promise<ApplyConfigurationResult> {

  const apply = await reloadConfiguration();

  // If every change applied live (or was rejected without needing a restart), there is nothing for the service manager to do; the in-memory CONFIG already
  // reflects the new values, and any registered handlers have made the live side-effects.
  if(apply.deferred.length === 0) {

    return { apply, restart: null };
  }

  // Some change could not be applied live - schedule a restart so the service manager picks up the new state on respawn.
  return { apply, restart: scheduleServerRestart(reason) };
}

/**
 * Builds the user-facing message for a save response based on the apply and restart outcome. Picks the strongest signal: a restart message when a restart
 * was scheduled (operators rely on this exact wording to recognize a pending restart), a live-applied summary when the change took effect immediately, or a
 * rejected summary when handlers refused the change. The message is plain prose - the structured counts go alongside in the data envelope for clients that
 * want them.
 * @param result - The combined apply and restart result.
 * @returns Single-sentence message describing the outcome.
 */
export function describeConfigurationOutcome(result: ApplyConfigurationResult): string {

  // A scheduled restart subsumes the live counts - the restart message already conveys what the operator needs to know, and its wording stays stable so
  // operators can recognize a pending restart at a glance.
  if(result.restart) {

    return result.restart.message;
  }

  const appliedCount = result.apply.applied.length;
  const rejectedCount = result.apply.rejected.length;

  if((appliedCount === 0) && (rejectedCount === 0)) {

    return "Configuration saved.";
  }

  if(rejectedCount === 0) {

    // Singular vs plural handled inline so the message reads naturally for a one-change save.
    return "Configuration saved. " + String(appliedCount) + " setting" + ((appliedCount === 1) ? "" : "s") + " applied live.";
  }

  // Surface the first rejection reason so operators get a directly actionable hint without scanning the structured payload.
  const firstReason = result.apply.rejected[0]?.reason ?? "unknown reason";

  return "Configuration saved, but " + String(rejectedCount) + " change" + ((rejectedCount === 1) ? " was" : "s were") + " rejected: " + firstReason + ".";
}

/**
 * Groups profiles by their declared category for UI display. Each profile declares its own category and this helper simply filters by that field. The record is
 * written out key by key rather than built from PROFILE_CATEGORIES, so a category added to the table without a bucket here is a compile error. Display order
 * belongs to the table, and every caller renders in it.
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
