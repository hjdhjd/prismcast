/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Configuration endpoint coordinator for PrismCast.
 */
import { LOG, isRunningAsService } from "../../utils/index.ts";
import { NEXT_STREAM_SCOPE, REACTIVITY_BADGES, formatSettingCount } from "./vocabulary.ts";
import type { Nullable, ProfileCategory } from "../../types/index.ts";
import { closeBrowser, emitCurrentSystemStatus } from "../../browser/index.ts";
import type { ApplyResult } from "../../config/reactivity.ts";
import type { Express } from "express";
import type { ProfileInfo } from "../../config/profiles.ts";
import type { UserConfig } from "../../config/userConfig.ts";
import { getStreamCount } from "../../streaming/registry.ts";
import { saveConfiguration } from "../../config/index.ts";
import { setupChannelRoutes } from "./channels/index.ts";
import { setupProfileRoutes } from "./services.ts";
import { setupSettingsRoutes } from "./settings.ts";
import { systemClock } from "homebridge-plugin-utils";

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
 * Schedules a server restart for a save that holds a restart-class change, after a brief delay so the response is sent first. Not running as a service, nothing
 * can restart the process, so the result asks the user to restart PrismCast for the settings marked Restart. Running as a service with active streams, the
 * restart is deferred until the streams end, so the client can show a dialog and let the user choose to wait or force it. Otherwise the browser closes and the
 * process exits for the service manager to start it again.
 * @param reason - A description of why the server is restarting, used in the log message.
 * @returns Information about the restart including the message to display and whether auto-restart will occur.
 */
export function scheduleServerRestart(reason: string): RestartResult {

  const willRestart = isRunningAsService();

  // When not running as a service, nothing can restart the process for the user, so the message asks for a manual restart of the settings marked Restart.
  if(!willRestart) {

    LOG.info("Configuration saved %s. The settings marked %s take effect after a manual restart.", reason, REACTIVITY_BADGES.restart.label);

    return {

      activeStreams: 0,
      deferred: false,
      message: "Configuration saved. Restart PrismCast for the settings marked " + REACTIVITY_BADGES.restart.label + " to take effect.",
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
  systemClock.schedule(() => {

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
 * Saves a change to the settings surface through the one validated save and schedules a server restart only when the save holds a restart-class change of its
 * own for a restart. Every writer of the settings surface - the /config save, the /config/import handler, and the debug page - calls it with the mutation it
 * wants applied to the file. The returned shape lets each caller tailor its response message and pick between the "show toast" and "restart in progress" UI
 * flows, and lets the debug page, whose redirect carries no response body, log the outcome instead.
 *
 * Rejected changes do not trigger a restart on their own - rejection means a handler refused the change after the disk write, so the value is persisted but
 * the live side-effect did not occur (e.g., a handler that refused to start a port-conflicting server). Callers should surface rejected reasons to the user so
 * they can fix the underlying cause and re-save rather than restarting blindly.
 *
 * Once the reconcile resolves, the save composes the current system status for the page header, so a saved stream limit reaches every connected tab without a
 * reload; the status dedupe turns that emission into nothing when no field the header renders changed.
 * @param reason - A description of why configuration is changing, used in the restart log message when a restart is scheduled.
 * @param mutator - Applies the change to the current configuration file in place.
 * @returns Combined apply and restart result.
 * @throws ConfigurationRejectedError when the saved configuration would fail validation, and the store's error when the file cannot be parsed, read, or written.
 */
export async function applyConfigurationChange(reason: string, mutator: (current: UserConfig) => void): Promise<ApplyConfigurationResult> {

  const apply = await saveConfiguration(mutator);

  // A save can change what the page header renders, since the stream limit is live. The caller of a status change owns its emission, as every other status
  // change's caller does, so the save composes the status here; the dedupe turns it into nothing when no field the header renders changed.
  void emitCurrentSystemStatus();

  // When this save holds nothing for a restart, there is nothing for the service manager to do: what the save asked for is realized or reported refused.
  if(apply.deferred.length === 0) {

    return { apply, restart: null };
  }

  // This save introduced a restart-class change - schedule a restart so the service manager picks up the new state on respawn.
  return { apply, restart: scheduleServerRestart(reason) };
}

/**
 * Builds the user-facing message for a save response from the apply and restart outcome, picking the strongest signal: a scheduled restart's own message;
 * otherwise, when a handler refused a change, the count of refusals with the first reason; otherwise the saved sentence, followed by one sentence for the
 * settings applied live and one for the settings that apply to streams started after the save, each present only when its list is non-empty. Every surface
 * that reports a save reads this one composer, so they all say the same thing.
 * @param result - The combined apply and restart result.
 * @returns The message describing the outcome.
 */
export function describeConfigurationOutcome(result: ApplyConfigurationResult): string {

  // A scheduled restart's message already tells the user what this save needs from them, so it stands alone.
  if(result.restart) {

    return result.restart.message;
  }

  const { applied, nextStream, rejected } = result.apply;

  // Surface the first rejection reason so the user gets a directly actionable hint without scanning the structured payload. Every reason is a complete
  // sentence ending in its own punctuation, so the message carries it verbatim.
  if(rejected.length > 0) {

    const firstReason = rejected[0]?.reason ?? "The reason was not reported.";

    return "Configuration saved, but " + String(rejected.length) + " change" + ((rejected.length === 1) ? " was" : "s were") + " rejected: " + firstReason;
  }

  const sentences = ["Configuration saved."];

  if(applied.length > 0) {

    sentences.push(formatSettingCount(applied.length) + " applied live.");
  }

  // The next-stream sentence agrees its verb with the count, since its subject is the count phrase itself.
  if(nextStream.length > 0) {

    sentences.push(formatSettingCount(nextStream.length) + " " + ((nextStream.length === 1) ? "applies" : "apply") + " to " + NEXT_STREAM_SCOPE + ".");
  }

  return sentences.join(" ");
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
export { collectPendingSettings, generateAdvancedTabContent, generateCollapsibleSection, generateSettingsFormFooter, generateSettingsTabContent } from "./settings.ts";
export { generateCustomProfilesPanel, generateProfileWizardModal } from "./services.ts";
