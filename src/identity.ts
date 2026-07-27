/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * identity.ts: Project-wide identity constants for PrismCast.
 */

/* This module is the single source of truth for the values other modules need to refer to PrismCast by name: the reverse-DNS service identifier the macOS
 * launchd plist carries, and the human-readable product name the Linux systemd unit, the Windows Task Scheduler task, and UI surfaces display. It sits at the
 * root of src/ - below config, utils, service, and every other subsystem - because both the path-resolution layer (config/paths.ts, which uses SERVICE_ID in
 * the macOS LaunchAgents path) and the service-generation layer (service/generators.ts, which embeds SERVICE_ID in the launchd plist and SERVICE_NAME in the
 * systemd unit and the Task Scheduler task) consume it. Placing it inside either of those subsystems would force the other to import sideways across a
 * boundary it otherwise respects; lifting it to a dedicated root-level identity module preserves the layering and gives the constants a home that names what
 * they are rather than where they happened to first be used.
 */

// The reverse-DNS service identifier. macOS launchd uses this for the bundle name, the LaunchAgents plist file name, and the Label key inside the plist itself,
// and it is also the identifier passed to launchctl start/stop/remove commands. Linux systemd and Windows Task Scheduler do not consume this value at all -
// they identify the service by SERVICE_NAME instead, since neither service manager requires reverse-DNS form for its unit or task identifiers.
export const SERVICE_ID = "com.github.hjdhjd.prismcast";

// The human-readable product name. Surfaced in UI labels, in the service-install command output, in the Linux systemd unit's Description line, and as the
// task name in the Windows Task Scheduler generator. macOS is the clearest example of a platform that separates "internal identifier" from "shown name": the
// launchd plist stores only SERVICE_ID and never SERVICE_NAME, so the human-readable name exists only in this module and the CLI output that references it.
export const SERVICE_NAME = "PrismCast";
