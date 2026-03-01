/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.ts: Landing page route for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { checkForUpdates, getChangelogItems, getPackageVersion, getVersionInfo } from "../../utils/index.js";
import { generateApiReferenceContent, generateChannelsTabContent, generateConfigContent, generateHelpContent, generateLogsContent,
  generateOverviewContent } from "./content.js";
import { generateBaseStyles, generatePageWrapper, generateTabButton, generateTabPanel, generateTabScript, generateTabStyles } from "../ui.js";
import { generateChannelsSubtabScript, generateConfigSubtabScript, generateStatusScript } from "./scripts/index.js";
import { generateLandingPageStyles } from "./styles.js";
import { resolveBaseUrl } from "../playlist.js";

/* The landing page provides operators with all the information they need to integrate with Channels DVR. It features a tabbed interface with six sections:
 *
 * 1. Overview - Introduction to PrismCast and Quick Start instructions
 * 2. Channels - The full M3U playlist with copy functionality
 * 3. Logs - Real-time log viewer for troubleshooting
 * 4. Configuration - Channel management and settings (with subtabs)
 * 5. API Reference - Documentation for all HTTP endpoints
 * 6. Help - Updating, platform notes, troubleshooting, and known limitations
 */

/**
 * Generates the system status bar HTML for the page header.
 * @returns HTML content for the system status bar.
 */
function generateHeaderStatusHtml(): string {

  return [
    "<div id=\"system-status\" class=\"header-status\">",
    "<span id=\"system-health\"><span class=\"status-dot\" style=\"color: var(--text-muted);\">&#9679;</span> Connecting...</span>",
    "<div class=\"dropdown stream-popover\">",
    "<button type=\"button\" id=\"stream-count\" aria-label=\"Active streams\" onclick=\"toggleStreamPopover()\">-</button>",
    "<div class=\"dropdown-menu\" id=\"stream-popover-menu\"></div>",
    "</div>",
    "</div>"
  ].join("\n");
}

/**
 * Generates the version display HTML with update indicator if available.
 * @returns HTML content for the version display.
 */
function generateVersionHtml(): string {

  const currentVersion = getPackageVersion();
  const versionInfo = getVersionInfo(currentVersion);

  // Refresh icon for manual update check (using Unicode refresh symbol).
  const refreshIcon = [
    "<button type=\"button\" class=\"version-check\" onclick=\"checkForUpdates()\" title=\"Check for updates\" aria-label=\"Check for updates\">",
    "&#8635;",
    "</button>"
  ].join("");

  if(versionInfo.updateAvailable && versionInfo.latestVersion) {

    // Update available - make version area clickable to open changelog modal, with refresh icon.
    return [
      "<span class=\"version-container\">",
      "<a href=\"#\" class=\"version version-update\" onclick=\"openChangelogModal(); return false;\">",
      "v" + currentVersion + " &rarr; v" + versionInfo.latestVersion,
      "</a>",
      refreshIcon,
      "</span>"
    ].join("");
  }

  // No update - show current version (clickable to view changelog) with refresh icon.
  return [
    "<span class=\"version-container\" id=\"version-display\">",
    "<a href=\"#\" class=\"version\" onclick=\"openChangelogModal(); return false;\">v" + currentVersion + "</a>",
    refreshIcon,
    "</span>"
  ].join("");
}

/**
 * Generates the changelog modal HTML with placeholder content. The actual changelog is fetched dynamically when the modal opens.
 * @returns HTML content for the changelog modal.
 */
function generateChangelogModal(): string {

  return [
    "<div id=\"changelog-modal\" class=\"changelog-modal\">",
    "<div class=\"changelog-modal-content\">",
    "<h3 class=\"changelog-title\">What's new</h3>",
    "<div class=\"changelog-loading\">Loading...</div>",
    "<div class=\"changelog-content\" style=\"display: none;\"></div>",
    "<p class=\"changelog-error\" style=\"display: none;\">Unable to load changelog.</p>",
    "<div class=\"changelog-modal-buttons\">",
    "<button type=\"button\" id=\"changelog-upgrade-btn\" class=\"btn btn-success\" style=\"display: none;\" onclick=\"startUpgrade()\">Upgrade</button>",
    "<a href=\"https://github.com/hjdhjd/prismcast/releases\" target=\"_blank\" rel=\"noopener\" class=\"btn btn-primary\">View on GitHub</a>",
    "<button type=\"button\" class=\"btn btn-secondary\" onclick=\"closeChangelogModal()\">Close</button>",
    "</div>",
    "</div>",
    "</div>"
  ].join("\n");
}

/**
 * Configures the root endpoint that serves as a landing page with a tabbed interface containing usage documentation, API reference, playlist, and log viewer.
 * @param app - The Express application.
 */
export function setupRootEndpoint(app: Express): void {

  // Manual version check endpoint.
  app.post("/version/check", async (_req: Request, res: Response): Promise<void> => {

    const currentVersion = getPackageVersion();

    await checkForUpdates(currentVersion, true);

    const versionInfo = getVersionInfo(currentVersion);

    res.json({

      currentVersion,
      latestVersion: versionInfo.latestVersion,
      updateAvailable: versionInfo.updateAvailable
    });
  });

  // Changelog fetch endpoint. Returns changelog items for the appropriate version (latest if update available, otherwise current). Falls back to current version's
  // changelog if the latest version's changelog isn't available.
  app.get("/version/changelog", async (_req: Request, res: Response): Promise<void> => {

    const currentVersion = getPackageVersion();
    const versionInfo = getVersionInfo(currentVersion);

    // Prefer latest version's changelog if update available, otherwise use current version.
    let displayVersion = (versionInfo.updateAvailable && versionInfo.latestVersion) ? versionInfo.latestVersion : currentVersion;
    let items = await getChangelogItems(displayVersion);

    // Fallback: if latest version's changelog not found, try current version instead. Update displayVersion immediately so it reflects what we're actually
    // attempting to show, even if the fallback also fails.
    if((items === null) && (displayVersion !== currentVersion)) {

      displayVersion = currentVersion;
      items = await getChangelogItems(currentVersion);
    }

    res.json({

      displayVersion,
      items,
      updateAvailable: versionInfo.updateAvailable
    });
  });

  app.get("/", (req: Request, res: Response): void => {

    const baseUrl = resolveBaseUrl(req);

    // Generate content for each tab.
    const overviewContent = generateOverviewContent(baseUrl);
    const channelsContent = generateChannelsTabContent();
    const logsContent = generateLogsContent();
    const configContent = generateConfigContent();
    const apiContent = generateApiReferenceContent();
    const helpContent = generateHelpContent();

    // Build the tab bar.
    const tabBar = [
      "<div class=\"tab-bar\" role=\"tablist\">",
      generateTabButton("overview", "Overview", true),
      generateTabButton("channels", "Channels", false),
      generateTabButton("logs", "Logs", false),
      generateTabButton("config", "Configuration", false),
      generateTabButton("api", "API Reference", false),
      generateTabButton("help", "Help", false),
      "</div>"
    ].join("\n");

    // Build the tab panels.
    const tabPanels = [
      generateTabPanel("overview", overviewContent, true),
      generateTabPanel("channels", channelsContent, false),
      generateTabPanel("logs", logsContent, false),
      generateTabPanel("config", configContent, false),
      generateTabPanel("api", apiContent, false),
      generateTabPanel("help", helpContent, false)
    ].join("\n");

    // Build the page header with logo, title, version, links, and status bar.
    const header = [
      "<div class=\"header\">",
      "<div class=\"header-left\">",
      "<img src=\"/logo.svg\" alt=\"PrismCast\" class=\"logo\">",
      "<h1>PrismCast</h1>",
      generateVersionHtml(),
      "<span class=\"header-links\">",
      "<a href=\"https://github.com/hjdhjd/prismcast\" target=\"_blank\" rel=\"noopener\">GitHub</a>",
      "<span class=\"header-links-sep\">&middot;</span>",
      "<a href=\"https://github.com/hjdhjd\" target=\"_blank\" rel=\"noopener\">More by HJD</a>",
      "</span>",
      "</div>",
      generateHeaderStatusHtml(),
      "</div>"
    ].join("\n");

    // Combine all styles.
    const styles = [ generateBaseStyles(), generateTabStyles(), generateLandingPageStyles() ].join("\n");

    // Restart dialog modal HTML. This is rendered hidden and shown via JavaScript when a restart is deferred due to active streams.
    const restartModal = [
      "<div id=\"restart-dialog\" class=\"restart-modal\">",
      "<div class=\"restart-modal-content\">",
      "<h3>Restart Required</h3>",
      "<p>Configuration saved. <span id=\"restart-stream-count\">0</span> active stream(s) will be interrupted if you restart now.</p>",
      "<div class=\"restart-modal-status\">Waiting for streams to end...</div>",
      "<div class=\"restart-modal-buttons\">",
      "<button type=\"button\" class=\"btn btn-secondary\" onclick=\"cancelPendingRestart()\">Cancel</button>",
      "<button type=\"button\" class=\"btn btn-danger\" onclick=\"forceRestart()\">Restart Now</button>",
      "</div>",
      "</div>",
      "</div>"
    ].join("\n");

    // Build the body content.
    const changelogModal = generateChangelogModal();
    const bodyContent = [ header, tabBar, tabPanels, restartModal, changelogModal,
      "<div id=\"toast-container\" class=\"toast-container\"></div>" ].join("\n");

    // Generate scripts: tab switching, config subtab handling, then status SSE for header updates.
    const scripts = [
      generateTabScript({ localStorageKey: "prismcast-home-tab" }),
      generateChannelsSubtabScript(),
      generateConfigSubtabScript(),
      generateStatusScript()
    ].join("\n");

    // Build and send the complete page.
    const html = generatePageWrapper("PrismCast", styles, bodyContent, scripts);

    res.send(html);
  });
}
