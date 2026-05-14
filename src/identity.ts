/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * identity.ts: Project-wide identity constants for PrismCast.
 */

/* This module is the single source of truth for the values other modules need to refer to PrismCast by name: the reverse-DNS service identifier launchd plists
 * and systemd units carry, and the human-readable product name UI surfaces display. It sits at the root of src/ - below config, utils, service, and every other
 * subsystem - because both the path-resolution layer (config/paths.ts, which uses SERVICE_ID in the macOS LaunchAgents path) and the service-generation layer
 * (service/generators.ts, which embeds both values in plist and unit files) consume it. Placing it inside either of those subsystems would force the other to
 * import sideways across a boundary it otherwise respects; lifting it to a dedicated root-level identity module preserves the layering and gives the constants
 * a home that names what they are rather than where they happened to first be used.
 */

// The reverse-DNS service identifier. macOS launchd uses this for the bundle name, the LaunchAgents plist file name, and the Label key inside the plist itself.
// Linux systemd and Windows Task Scheduler do not require reverse-DNS form, but the generators still quote this value in the unit/task definitions so users see
// a consistent identifier across platforms when inspecting or removing the installed service.
export const SERVICE_ID = "com.github.hjdhjd.prismcast";

// The human-readable product name. Surfaced in UI labels, in the service-install command output, and as the display name on platforms whose service managers
// distinguish "internal identifier" from "shown name" (Windows Task Scheduler being the clearest example).
export const SERVICE_NAME = "PrismCast";
