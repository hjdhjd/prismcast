/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * root.ts: Landing page route for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { checkForUpdates, escapeHtml, getChangelogItems, getPackageVersion, getVersionInfo, isRunningAsService } from "../utils/index.js";
import { generateAdvancedTabContent, generateChannelsPanel, generateSettingsFormFooter, generateSettingsTabContent, hasEnvOverrides } from "./config.js";
import { generateBaseStyles, generatePageWrapper, generateTabButton, generateTabPanel, generateTabScript, generateTabStyles } from "./ui.js";
import { VIDEO_QUALITY_PRESETS } from "../config/presets.js";
import { getAllChannels } from "../config/userChannels.js";
import { getUITabs } from "../config/userConfig.js";
import { resolveBaseUrl } from "./playlist.js";
import { resolveProfile } from "../config/profiles.js";

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
    "<span id=\"stream-count\" onclick=\"toggleStreamPopover()\">-</span>",
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
    "<button type=\"button\" class=\"version-check\" onclick=\"checkForUpdates()\" title=\"Check for updates\">",
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
 * Generates the active streams table for the Overview tab.
 * @returns HTML content for the active streams section.
 */
function generateActiveStreamsSection(): string {

  return [
    "<div id=\"streams-container\">",
    "<table id=\"streams-table\" class=\"streams-table\">",
    "<tbody id=\"streams-tbody\">",
    "<tr class=\"empty-row\"><td colspan=\"4\">No active streams</td></tr>",
    "</tbody>",
    "</table>",
    "</div>"
  ].join("\n");
}

/**
 * Generates the JavaScript for status SSE connection and UI updates. This script runs at page level to keep the header status updated across all tabs.
 * @returns JavaScript code as a string wrapped in script tags.
 */
function generateStatusScript(): string {

  return [
    "<script>",
    "var statusEventSource = null;",
    "var streamData = {};",
    "var systemData = null;",
    "var expandedStreams = {};",
    "var healthColorVars = { healthy: 'var(--stream-healthy)', buffering: 'var(--stream-buffering)', recovering: 'var(--stream-recovering)', ",
    "  stalled: 'var(--stream-stalled)', error: 'var(--stream-error)' };",

    // Format duration in human readable format.
    "function formatDuration(seconds) {",
    "  if (seconds < 60) return seconds + 's';",
    "  if (seconds < 3600) return Math.floor(seconds / 60) + 'm ' + (seconds % 60) + 's';",
    "  var h = Math.floor(seconds / 3600);",
    "  var m = Math.floor((seconds % 3600) / 60);",
    "  return h + 'h ' + m + 'm';",
    "}",

    // Format bytes in human readable format.
    "function formatBytes(bytes) {",
    "  if (bytes === 0) return '0 B';",
    "  if (bytes < 1024) return bytes + ' B';",
    "  if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';",
    "  return (bytes / 1048576).toFixed(1) + ' MB';",
    "}",

    // Format absolute time (e.g., "6:54 AM" or "Jan 14, 6:54 AM" if different day).
    "function formatTime(isoString) {",
    "  var date = new Date(isoString);",
    "  var now = new Date();",
    "  var hours = date.getHours();",
    "  var minutes = date.getMinutes();",
    "  var ampm = hours >= 12 ? 'PM' : 'AM';",
    "  hours = hours % 12;",
    "  hours = hours ? hours : 12;",
    "  var timeStr = hours + ':' + (minutes < 10 ? '0' : '') + minutes + ' ' + ampm;",
    "  if (date.toDateString() !== now.toDateString()) {",
    "    var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];",
    "    timeStr = months[date.getMonth()] + ' ' + date.getDate() + ', ' + timeStr;",
    "  }",
    "  return timeStr;",
    "}",

    // Extract concise domain from URL for display (last two hostname parts). Mirrors the server-side extractDomain() in utils/format.ts.
    "function getDomain(url) {",
    "  try {",
    "    var parts = new URL(url).hostname.split('.');",
    "    return parts.length > 2 ? parts.slice(-2).join('.') : parts.join('.');",
    "  } catch (e) {",
    "    return url;",
    "  }",
    "}",

    // Build channel display HTML with an optional logo image. When a logo URL is available, renders an img element with an onerror fallback that hides the
    // image and reveals a text span. The logoClass and textClass parameters allow callers to apply context-specific sizing.
    "function channelDisplayHtml(logoUrl, name, logoClass, textClass) {",
    "  if(logoUrl) {",
    "    return '<img src=\"' + logoUrl + '\" class=\"' + logoClass + '\" alt=\"' + name + '\" title=\"' + name + '\" ' +",
    "      'onerror=\"this.style.display=\\'none\\';this.nextElementSibling.style.display=\\'inline\\'\">' +",
    "      '<span class=\"' + textClass + '\" style=\"display:none\">' + name + '</span>';",
    "  }",
    "  return '<span class=\"' + textClass + '\">' + name + '</span>';",
    "}",

    // Get row background color based on health status. Uses CSS variables for theme support.
    "function getRowTint(health) {",
    "  var tints = {",
    "    healthy: 'transparent',",
    "    buffering: 'var(--stream-tint-buffering)',",
    "    stalled: 'var(--stream-tint-stalled)',",
    "    recovering: 'var(--stream-tint-recovering)',",
    "    error: 'var(--stream-tint-error)'",
    "  };",
    "  return tints[health] || 'transparent';",
    "}",

    // Get health badge HTML using CSS variables for theme-aware colors. NOTE: Escalation level semantics defined in monitor.ts.
    // L1=play/unmute, L2=seek, L3=source reload, L4=page navigation.
    "function getHealthBadge(health, level) {",
    "  var label = '';",
    "  if (health === 'healthy') { label = 'Healthy'; }",
    "  else if (health === 'buffering') { label = 'Buffering'; }",
    "  else if (health === 'stalled') { label = 'Stalled'; }",
    "  else if (health === 'error') { label = 'Error'; }",
    "  else if (health === 'recovering') {",
    "    if (level === 1) { label = 'Resuming playback'; }",
    "    else if (level === 2) { label = 'Syncing to live'; }",
    "    else if (level === 3) { label = 'Reloading player'; }",
    "    else if (level >= 4) { label = 'Reloading page'; }",
    "    else { label = 'Recovering'; }",
    "  }",
    "  else { label = health; }",
    "  return '<span class=\"status-dot\" style=\"color: ' + (healthColorVars[health] || 'var(--text-muted)') + ';\">&#9679;</span> ' +",
    "    '<span style=\"color: var(--text-secondary);\">' + label + '</span>';",
    "}",

    // Update system status display in header. Shows system health (green dot when connected, red with label when not) and stream count.
    "function updateSystemStatus() {",
    "  if(!systemData) return;",
    "  var healthEl = document.getElementById('system-health');",
    "  var streamEl = document.getElementById('stream-count');",
    "  if(systemData.browser.connected) {",
    "    healthEl.innerHTML = '<span class=\"status-dot\" style=\"color: var(--stream-healthy);\">&#9679;</span>';",
    "  } else {",
    "    healthEl.innerHTML = '<span class=\"status-dot\" style=\"color: var(--stream-error);\">&#9679;</span> Browser offline';",
    "  }",
    "  var active = systemData.streams.active;",
    "  var limit = systemData.streams.limit;",
    "  if(active === 0) {",
    "    streamEl.textContent = '0 streams';",
    "    streamEl.classList.remove('clickable');",
    "    var popMenu = document.getElementById('stream-popover-menu');",
    "    if(popMenu) popMenu.classList.remove('show');",
    "  } else {",
    "    streamEl.textContent = active + '/' + limit + ' streams';",
    "    streamEl.classList.add('clickable');",
    "  }",
    "}",

    // Build the popover content from streamData. Populates the given menu element with one row per active stream.
    "function buildStreamPopoverContent(menu) {",
    "  var ids = Object.keys(streamData);",
    "  var html = '';",
    "  var now = Date.now();",
    "  for(var i = 0; i < ids.length; i++) {",
    "    var s = streamData[ids[i]];",
    "    var color = healthColorVars[s.health] || 'var(--text-muted)';",
    "    var name = s.channel || s.providerName || getDomain(s.url);",
    "    var dur = Math.floor((now - new Date(s.startTime).getTime()) / 1000);",
    "    var titleAttr = s.showName ? ' title=\"' + s.showName + '\"' : '';",
    "    html += '<div class=\"stream-popover-row\"' + titleAttr + '>';",
    "    html += '<span class=\"status-dot\" style=\"color: ' + color + ';\">&#9679;</span>';",
    "    html += channelDisplayHtml(s.logoUrl, name, 'stream-popover-logo', 'stream-popover-channel');",
    "    html += '<span class=\"stream-popover-duration\">' + formatDuration(dur) + '</span>';",
    "    html += '</div>';",
    "  }",
    "  menu.innerHTML = html;",
    "}",

    // Update an already-open stream popover with current data. Called from SSE handlers and the duration interval.
    "function updateStreamPopover() {",
    "  var menu = document.getElementById('stream-popover-menu');",
    "  if(!menu || !menu.classList.contains('show')) return;",
    "  var ids = Object.keys(streamData);",
    "  if(ids.length === 0) {",
    "    menu.classList.remove('show');",
    "    return;",
    "  }",
    "  buildStreamPopoverContent(menu);",
    "}",

    // Toggle the stream popover open or closed.
    "window.toggleStreamPopover = function() {",
    "  var ids = Object.keys(streamData);",
    "  if(ids.length === 0) return;",
    "  var menu = document.getElementById('stream-popover-menu');",
    "  if(!menu) return;",
    "  var isOpen = menu.classList.contains('show');",
    "  closeDropdowns();",
    "  if(!isOpen) {",
    "    buildStreamPopoverContent(menu);",
    "    menu.classList.add('show');",
    "  }",
    "};",

    // Format last issue for display.
    "function formatLastIssue(s) {",
    "  if (!s.lastIssueType || !s.lastIssueTime) { return 'None'; }",
    "  var issueLabel = s.lastIssueType.charAt(0).toUpperCase() + s.lastIssueType.slice(1);",
    "  var timeStr = formatTime(new Date(s.lastIssueTime).toISOString());",
    "  var status = (s.health === 'healthy') ? ' (recovered)' : ' (recovering)';",
    "  return issueLabel + ' at ' + timeStr + status;",
    "}",

    // Format auto-recovery info for display.
    "function formatAutoRecovery(s) {",
    "  var attempts = s.recoveryAttempts;",
    "  var reloads = s.pageReloadsInWindow;",
    "  if (attempts === 0) { return 'N/A'; }",
    "  var str = attempts + (attempts === 1 ? ' attempt' : ' attempts');",
    "  if (reloads > 0) { str += ', ' + reloads + (reloads === 1 ? ' page reload' : ' page reloads'); }",
    "  return str;",
    "}",

    // Format client type breakdown for the detail row.
    "function formatClients(s) {",
    "  if (s.clientCount === 0) { return 'None'; }",
    "  var labels = { 'hls': 'HLS', 'mpegts': 'MPEG-TS' };",
    "  var parts = [];",
    "  for (var i = 0; i < s.clients.length; i++) {",
    "    var c = s.clients[i];",
    "    parts.push(c.count + ' ' + (labels[c.type] || c.type));",
    "  }",
    "  return parts.join(', ');",
    "}",

    // Render the streams table.
    "function renderStreamsTable() {",
    "  var tbody = document.getElementById('streams-tbody');",
    "  if (!tbody) return;",
    "  var streamIds = Object.keys(streamData);",
    "  if (streamIds.length === 0) {",
    "    tbody.innerHTML = '<tr class=\"empty-row\"><td colspan=\"4\">No active streams</td></tr>';",
    "    return;",
    "  }",
    "  var html = '';",
    "  for (var i = 0; i < streamIds.length; i++) {",
    "    var id = streamIds[i];",
    "    var s = streamData[id];",
    "    var isExpanded = expandedStreams[id];",
    "    var chevron = isExpanded ? '&#9660;' : '&#9654;';",
    "    var rowTint = getRowTint(s.health);",
    "    var channelText = s.channel || s.providerName || getDomain(s.url);",
    "    var channelDisplay = channelDisplayHtml(s.logoUrl, channelText, 'channel-logo', 'channel-text');",
    "    html += '<tr class=\"stream-row\" data-id=\"' + id + '\" onclick=\"toggleStreamDetails(' + id + ')\" style=\"background-color: ' + rowTint + ';\">';",
    "    html += '<td class=\"chevron\">' + chevron + '</td>';",
    "    var durationSpan = '<span class=\"stream-duration\" id=\"duration-' + id + '\">\\u00b7 ' + formatDuration(s.duration) + '</span>';",
    "    html += '<td class=\"stream-info\">' + channelDisplay + ' ' + durationSpan + '</td>';",
    "    var showDisplay = s.showName ? s.showName : '';",
    "    html += '<td class=\"stream-show\">' + showDisplay + '</td>';",
    "    var clientIndicator = '';",
    "    if (s.clientCount > 0) {",
    "      var title = s.clientCount + (s.clientCount !== 1 ? ' clients' : ' client');",
    "      clientIndicator = '<span class=\"client-count\" title=\"' + title + '\">&#9673; ' + s.clientCount + '</span> ';",
    "    }",
    "    html += '<td class=\"stream-health\">' + clientIndicator + getHealthBadge(s.health, s.escalationLevel) + '</td>';",
    "    html += '</tr>';",
    "    if (isExpanded) {",
    "      html += '<tr class=\"stream-details\" data-id=\"' + id + '\">';",
    "      html += '<td colspan=\"4\">';",
    "      html += '<div class=\"details-content\">';",
    "      html += '<div class=\"details-header\">';",
    "      html += '<div class=\"details-url\">' + s.url + '</div>';",
    "      var clientSuffix = s.clientCount > 0 ? ' &middot; ' + formatClients(s) : '';",
    "      html += '<div class=\"details-started\"><strong>Started:</strong> ' + formatTime(s.startTime) + clientSuffix + '</div>';",
    "      html += '</div>';",
    "      html += '<div class=\"details-metrics\">';",
    "      html += '<div class=\"details-issue\"><strong>Last issue:</strong> ' + formatLastIssue(s) + '</div>';",
    "      html += '<div class=\"details-recovery\"><strong>Recovery:</strong> ' + formatAutoRecovery(s) + '</div>';",
    "      html += '<div class=\"details-memory\"><strong>Memory:</strong> ' + formatBytes(s.memoryBytes) + '</div>';",
    "      html += '</div>';",
    "      html += '</div>';",
    "      html += '</td></tr>';",
    "    }",
    "  }",
    "  tbody.innerHTML = html;",
    "}",

    // Toggle stream details.
    "function toggleStreamDetails(id) {",
    "  expandedStreams[id] = !expandedStreams[id];",
    "  renderStreamsTable();",
    "}",

    // Update stream durations every second. We calculate duration from the immutable startTime rather than incrementing a counter, ensuring the displayed duration is
    // always accurate regardless of any staleness in server-sent updates.
    "function updateDurations() {",
    "  var now = Date.now();",
    "  var streamIds = Object.keys(streamData);",
    "  for (var i = 0; i < streamIds.length; i++) {",
    "    var id = streamIds[i];",
    "    var s = streamData[id];",
    "    var durationSec = Math.floor((now - new Date(s.startTime).getTime()) / 1000);",
    "    var el = document.getElementById('duration-' + id);",
    "    if (el) el.textContent = '\\u00b7 ' + formatDuration(durationSec);",
    "  }",
    "  updateStreamPopover();",
    "}",

    // Track the last time any SSE event was received from the status stream. Used by the staleness checker to detect silently dead connections.
    "var lastStatusEventTime = Date.now();",
    "var hiddenSince = 0;",

    // Connect (or reconnect) to the status SSE stream. Closes any existing connection first so this is safe to call repeatedly.
    "function connectStatusSSE() {",
    "  if(statusEventSource) { statusEventSource.close(); }",
    "  statusEventSource = new EventSource('/streams/status');",
    "  lastStatusEventTime = Date.now();",

    // Local helper that registers an event listener and updates the staleness timestamp on every event. Handlers are optional so heartbeat can
    // be registered with just on('heartbeat') for pure keepalive tracking. The onerror handler stays outside this wrapper because errors must
    // not reset the staleness timer — a connection that only fires errors is still dead.
    "  function on(event, handler) {",
    "    statusEventSource.addEventListener(event, function(e) {",
    "      lastStatusEventTime = Date.now();",
    "      if(handler) { handler(e); }",
    "    });",
    "  }",
    "  on('heartbeat');",
    "  on('snapshot', function(e) {",
    "    var data = JSON.parse(e.data);",
    "    systemData = data.system;",
    "    streamData = {};",
    "    for (var i = 0; i < data.streams.length; i++) {",
    "      streamData[data.streams[i].id] = data.streams[i];",
    "    }",
    "    updateSystemStatus();",
    "    renderStreamsTable();",
    "    updateStreamPopover();",
    "  });",
    "  on('streamAdded', function(e) {",
    "    var s = JSON.parse(e.data);",
    "    streamData[s.id] = s;",
    "    renderStreamsTable();",
    "    updateStreamPopover();",
    "  });",
    "  on('streamRemoved', function(e) {",
    "    var data = JSON.parse(e.data);",
    "    delete streamData[data.id];",
    "    delete expandedStreams[data.id];",
    "    renderStreamsTable();",
    "    updateStreamPopover();",
    "    if (typeof pendingRestart !== 'undefined' && pendingRestart) {",
    "      updateRestartDialogStatus();",
    "    }",
    "  });",
    "  on('streamHealthChanged', function(e) {",
    "    var s = JSON.parse(e.data);",
    "    if (streamData[s.id]) {",
    "      streamData[s.id] = s;",
    "      renderStreamsTable();",
    "      updateStreamPopover();",
    "    }",
    "  });",
    "  on('systemStatusChanged', function(e) {",
    "    systemData = JSON.parse(e.data);",
    "    updateSystemStatus();",
    "  });",
    "  statusEventSource.onerror = function() {",
    "    document.getElementById('system-health').innerHTML = '<span class=\"status-dot\" style=\"color: var(--stream-stalled);\">&#9679;</span> Updates paused';",
    "  };",
    "}",

    // Initial connection and periodic timers.
    "connectStatusSSE();",
    "setInterval(updateDurations, 1000);",

    // Staleness detection: if no SSE event has arrived in 45 seconds, the connection is likely dead. Reconnect proactively.
    "setInterval(function() {",
    "  if((Date.now() - lastStatusEventTime) > 45000) { connectStatusSSE(); }",
    "}, 45000);",

    // Visibility-driven reconnect. When the page returns from being hidden for more than 30 seconds, reconnect the status stream and re-activate
    // the current tab so the logs stream reconnects naturally through its existing tabactivated listener.
    "document.addEventListener('visibilitychange', function() {",
    "  if(document.hidden) {",
    "    hiddenSince = Date.now();",
    "  } else if((hiddenSince > 0) && ((Date.now() - hiddenSince) > 30000)) {",
    "    hiddenSince = 0;",
    "    connectStatusSSE();",
    "    var activeTab = document.querySelector('.tab-btn.active');",
    "    if(activeTab) {",
    "      document.dispatchEvent(new CustomEvent('tabactivated', { detail: { category: activeTab.getAttribute('data-category') } }));",
    "    }",
    "  } else {",
    "    hiddenSince = 0;",
    "  }",
    "});",

    // Copy playlist URL function for Overview tab Quick Start section.
    "window.copyOverviewPlaylistUrl = function() {",
    "  var urlEl = document.getElementById('overview-playlist-url');",
    "  if (urlEl) {",
    "    navigator.clipboard.writeText(urlEl.textContent).then(function() {",
    "      var feedback = document.getElementById('overview-copy-feedback');",
    "      if (feedback) {",
    "        feedback.style.display = 'inline';",
    "        setTimeout(function() { feedback.style.display = 'none'; }, 2000);",
    "      }",
    "    });",
    "  }",
    "};",

    "</script>"
  ].join("\n");
}

/**
 * Generates the Overview tab content with a comprehensive user guide covering what PrismCast is, video quality expectations, quick start instructions, tuning speed,
 * channel authentication, working with channels, and system requirements.
 * @param baseUrl - The base URL for the server.
 * @param videoChannelCount - The number of video channels available.
 * @returns HTML content for the Overview tab.
 */
function generateOverviewContent(baseUrl: string, videoChannelCount: number): string {

  return [

    // Active streams table at the top.
    generateActiveStreamsSection(),

    // What Is PrismCast?
    "<div class=\"section\">",
    "<h3>What Is PrismCast?</h3>",
    "<p>PrismCast captures live video from web-based TV players by driving a real Chrome browser. It navigates to streaming sites, captures the ",
    "screen and audio output, and serves the result as HLS streams over HTTP. Think of it as a <strong>virtual TV tuner for web-based content</strong> &mdash; ",
    "it lets Channels DVR (and other applications) record and watch content from streaming sites that do not offer direct video URLs.</p>",
    "<p>PrismCast is built around three priorities, in order:</p>",
    "<ol>",
    "<li><strong>Reliability</strong> &mdash; tuning a channel always delivers that channel. When the primary approach fails, fallback strategies ",
    "ensure the tune still succeeds.</li>",
    "<li><strong>Health monitoring</strong> &mdash; once a channel is playing, PrismCast continuously monitors the stream and takes corrective ",
    "action automatically if issues arise.</li>",
    "<li><strong>Speed</strong> &mdash; tuning and recovery should be as fast as possible, but never at the expense of reliability.</li>",
    "</ol>",
    "<p>The ordering is intentional. PrismCast will always choose the reliable path over the fast one.</p>",
    "</div>",

    // Video Quality.
    "<div class=\"section\">",
    "<h3>Video Quality</h3>",
    "<p><strong>PrismCast delivers H.264 video with AAC stereo audio</strong> at configurable quality presets ranging from 480p to 1080p. ",
    "Quality presets can be changed in the <a href=\"#config/settings\">Configuration</a> tab.</p>",
    "<p>This is <em>not</em> a replacement for native 4K, HDR, Dolby Vision, or surround sound &mdash; it is screen capture, not a direct feed. ",
    "PrismCast captures directly from Chrome's media pipeline with <strong>no video transcoding</strong>, which is why tuning is fast and CPU usage ",
    "stays low. The result is good quality video that works well for everyday viewing and DVR recording. PrismCast is designed for content you ",
    "<strong>cannot get any other way</strong> in Channels DVR: network streaming sites, free ad-supported TV, and live channels that only exist on the web.</p>",
    "</div>",

    // Quick Start (Channels DVR).
    "<div class=\"section\">",
    "<h3>Quick Start</h3>",
    "<p>To add PrismCast channels to Channels DVR:</p>",
    "<ol>",
    "<li>Go to <strong>Settings &rarr; Custom Channels</strong> in your Channels DVR server.</li>",
    "<li>Click <strong>Add Source</strong> and select <strong>M3U Playlist</strong>.</li>",
    "<li>Enter the playlist URL: <code id=\"overview-playlist-url\">" + baseUrl + "/playlist</code> ",
    "<button class=\"btn-copy-inline\" onclick=\"copyOverviewPlaylistUrl()\" title=\"Copy URL\">Copy</button>",
    "<span id=\"overview-copy-feedback\" class=\"copy-feedback-inline\">Copied!</span></li>",
    "<li>Set <strong>Stream Format</strong> to <strong>HLS</strong>.</li>",
    "<li>The " + String(videoChannelCount) + " configured channels will be imported automatically.</li>",
    "</ol>",
    "<p>Individual channels can also be streamed directly using HLS URLs like <code>" + baseUrl + "/hls/nbc/stream.m3u8</code>.</p>",
    "</div>",

    // Plex Integration.
    "<div class=\"section\">",
    "<h3>Plex Integration</h3>",
    "<p>PrismCast includes built-in HDHomeRun emulation, allowing Plex to use it as a network tuner for live TV and DVR recording.</p>",
    "<ol>",
    "<li>In Plex, go to <strong>Settings &rarr; Live TV &amp; DVR &rarr; Set Up Plex DVR</strong>.</li>",
    "<li>Enter your PrismCast server address with port 5004 (e.g., <code>192.168.1.100:5004</code>).</li>",
    "<li>Plex will detect PrismCast as an HDHomeRun tuner and import available channels.</li>",
    "</ol>",
    "<p>HDHomeRun emulation is enabled by default and can be configured in the ",
    "<a href=\"#config/settings\">HDHomeRun / Plex</a> configuration tab.</p>",
    "</div>",

    // Tuning Speed.
    "<div class=\"section\">",
    "<h3>Tuning Speed</h3>",
    "<p>When a client requests a channel, PrismCast navigates Chrome to the streaming site, locates the video player, starts capture, and serves the ",
    "first HLS segment. How long this takes depends on the channel type:</p>",

    "<h4>Direct URL Channels (~3&ndash;5 seconds)</h4>",
    "<p>Sites where PrismCast navigates directly to a player page and video starts automatically. ",
    "Examples: NBC, ABC, Paramount+, USA Network.</p>",

    "<h4>Guide-Based Providers &mdash; First Tune (~5&ndash;10 seconds)</h4>",
    "<p>Sites where PrismCast navigates a live TV guide to find and select the channel. The first tune for a given channel is slower because the ",
    "guide grid must be searched. Examples: HBO Max, Hulu, Sling TV, YouTube TV, Fox.</p>",

    "<h4>Guide-Based Providers &mdash; Subsequent Tunes (~3&ndash;5 seconds)</h4>",
    "<p>After the first tune, PrismCast caches channel data for <strong>HBO Max, Hulu, Sling TV, and YouTube TV</strong>. ",
    "Subsequent tunes skip guide navigation entirely and are comparable to direct URL channels. If cached data ",
    "becomes stale, PrismCast falls back to guide navigation transparently.</p>",

    "<h4>Idle Window</h4>",
    "<p>Streams stay alive for <strong>30 seconds</strong> after the last client disconnects (configurable in the ",
    "<a href=\"#config/settings\">Configuration</a> tab). This means channel surfing in Channels DVR is instant for recently-viewed channels &mdash; ",
    "no re-tuning is needed. Combined with channel caching, the system gets faster the more you use it.</p>",
    "</div>",

    // Channel Authentication.
    "<div class=\"section\">",
    "<h3>Channel Authentication</h3>",
    "<p>Many streaming channels require TV provider authentication before content can be accessed. To authenticate:</p>",
    "<ol>",
    "<li>Go to the <a href=\"#channels\">Channels tab</a>.</li>",
    "<li>Click the <strong>Login</strong> button next to the channel you want to authenticate.</li>",
    "<li>A browser window will open with the channel's streaming page.</li>",
    "<li>Complete the TV provider sign-in process in the browser.</li>",
    "<li>Click <strong>Done</strong> when authentication is complete.</li>",
    "</ol>",
    "<p>Your login credentials are saved in the browser profile and persist across restarts. You only need to authenticate once per TV provider. ",
    "The Login button is stateless and always displays &ldquo;Login&rdquo; regardless of authentication status &mdash; successful authentication is ",
    "confirmed when the channel streams correctly. Some TV providers periodically expire sessions on their end, requiring re-authentication. This is ",
    "a provider limitation, not a PrismCast issue &mdash; simply click Login again to re-authenticate.</p>",
    "<p class=\"description-hint\">If PrismCast is running headless or on a remote server, use a VNC client to access the browser for authentication.</p>",
    "</div>",

    // Working with Channels.
    "<div class=\"section\">",
    "<h3>Working with Channels</h3>",

    "<h4>Predefined Channels</h4>",
    "<p>PrismCast ships with channels across multiple streaming providers, maintained and updated with each release. You can disable any channels ",
    "you do not need from the <a href=\"#channels\">Channels tab</a>. The predefined set covers common networks and is a good starting point &mdash; ",
    "enable what you watch and disable the rest. You can also override any predefined channel with your own custom definition ",
    "(see <em>Overriding Predefined Channels</em> below).</p>",

    "<h4>Provider Variants</h4>",
    "<p>Some channels (ESPN, Fox, NBC, etc.) are available from multiple streaming providers. The <strong>provider dropdown</strong> on each channel ",
    "lets you choose which service to use for that channel. Different providers may offer different tuning performance.</p>",

    "<h4>Provider Filter</h4>",
    "<p>If you only subscribe to certain streaming services, use the <strong>provider filter</strong> on the ",
    "<a href=\"#channels\">Channels tab</a> toolbar to show only relevant channels. This filter also applies to the M3U playlist, so Channels DVR ",
    "only imports channels from providers you actually use. You can also filter programmatically using the <code>?provider=</code> query parameter ",
    "on the playlist URL.</p>",

    "<h4>Bulk Operations</h4>",
    "<p>The <strong>Set all channels to</strong> dropdown on the <a href=\"#channels\">Channels tab</a> toolbar switches every multi-provider channel ",
    "to a single provider at once. This is useful when you want all channels routed through one streaming service. The operation can be undone by ",
    "switching individual channels back or selecting a different provider from the same dropdown.</p>",

    "<h4>User-Defined Channels</h4>",
    "<p>You can add custom channels for any streaming site. Provide a URL, select a site profile, and PrismCast will capture it. For sites with ",
    "multiple live channels (like a live TV provider), the <strong>Channel Selector</strong> field tells PrismCast which channel to tune to &mdash; ",
    "the expected value depends on the provider. When adding or editing a channel, select a profile to see the <strong>Profile Reference</strong> ",
    "section with site-specific guidance, including expected channel selector formats for known providers.</p>",

    "<h4>Overriding Predefined Channels</h4>",
    "<p>To override a predefined channel, create a user-defined channel with the same channel key. Both versions will appear in the provider ",
    "dropdown &mdash; yours labeled <em>Custom</em> and the original with its provider name. You can switch between them at any time.</p>",
    "<p class=\"description-hint\">For automation and integration with other workflows, see the <a href=\"#api\">API Reference</a> tab for the full HTTP API.</p>",
    "</div>",

    // Requirements.
    "<div class=\"section\">",
    "<h3>Requirements</h3>",
    "<ul>",
    "<li>Google Chrome browser installed.</li>",
    "<li>Sufficient memory for browser automation (2GB+ recommended).</li>",
    "<li>Network access to streaming sites.</li>",
    "</ul>",
    "<p class=\"description-hint\">See the <a href=\"#help\">Help</a> tab for platform-specific requirements and troubleshooting.</p>",
    "</div>"
  ].join("\n");
}

/**
 * Generates the Help tab content with updating instructions, platform notes, troubleshooting, and known limitations.
 * @returns HTML content for the Help tab.
 */
function generateHelpContent(): string {

  return [

    // Updating PrismCast.
    "<div class=\"section\">",
    "<h3>Updating PrismCast</h3>",
    "<p>Settings and channel configurations are preserved across updates.</p>",
    "<h4>Homebrew (macOS)</h4>",
    "<pre>brew upgrade prismcast\nprismcast service restart</pre>",
    "<h4>npm</h4>",
    "<pre>npm install -g prismcast\nprismcast service restart</pre>",
    "<h4>Docker</h4>",
    "<p>Pull the latest image and recreate the container. If using Watchtower, updates are applied automatically.</p>",
    "<pre>docker pull ghcr.io/hjdhjd/prismcast:latest\ndocker compose up -d</pre>",
    "</div>",

    // Display and Resolution.
    "<div class=\"section\">",
    "<h3>Display and Resolution</h3>",
    "<p>PrismCast captures video from Chrome's display output. The <strong>capture resolution must be smaller than the physical display resolution</strong> ",
    "because browser toolbars and window chrome consume approximately 100&ndash;150 vertical pixels. For example, to capture at 1080p (1920&times;1080), the ",
    "display must be larger than 1080p.</p>",
    "<p>When the selected quality preset exceeds what the display can provide, PrismCast logs a warning and automatically degrades to the best available preset. ",
    "This is not an error &mdash; PrismCast is adapting to your display.</p>",
    "<h4>Headless Servers</h4>",
    "<p>macOS works without a physical monitor. Windows and Linux servers without a display need an <strong>HDMI dummy plug</strong> or a ",
    "<strong>virtual display adapter</strong> to provide a display resolution for Chrome to render into.</p>",
    "<h4>Remote Access</h4>",
    "<p>macOS Screen Sharing and VNC work correctly. <strong>Windows Remote Desktop (RDP) does not work</strong> &mdash; RDP creates a virtual display ",
    "with different properties that interfere with Chrome's rendering. Use VNC or connect a physical display on Windows.</p>",
    "</div>",

    // Platform Notes.
    "<div class=\"section\">",
    "<h3>Platform Notes</h3>",
    "<h4>macOS</h4>",
    "<p>Chrome on macOS uses GPU hardware acceleration for video encoding, providing the best capture performance. After installing Node.js, go to ",
    "<strong>System Settings &rarr; Privacy &amp; Security &rarr; App Management</strong> and allow Node.js. Use Screen Sharing or VNC for remote access ",
    "to the PrismCast machine.</p>",
    "<h4>Windows</h4>",
    "<p>Install PrismCast as a service with <code>prismcast service install</code>. See Remote Access above for display capture requirements.</p>",
    "<h4>Linux / Docker</h4>",
    "<p>Chrome cannot use GPU hardware acceleration with virtual displays on Linux (a Chrome limitation), so Docker containers rely on software ",
    "rendering. Access the browser via VNC for authentication &mdash; Docker containers expose noVNC at port 6080.</p>",
    "</div>",

    // Troubleshooting.
    "<div class=\"section\">",
    "<h3>Troubleshooting</h3>",
    "<table>",
    "<tr><th>Problem</th><th>Cause</th><th>Solution</th></tr>",
    "<tr>",
    "<td>\"Browser Offline\" or \"Browser is not connected\"</td>",
    "<td>An existing Chrome process is running.</td>",
    "<td>Quit all Chrome instances, then restart PrismCast.</td>",
    "</tr>",
    "<tr>",
    "<td>\"All tuners in use\" despite no active streams</td>",
    "<td>Stale stream state.</td>",
    "<td>Restart PrismCast service.</td>",
    "</tr>",
    "<tr>",
    "<td>Chrome won't open for login</td>",
    "<td>Running headless or as a service.</td>",
    "<td>Access the PrismCast machine via VNC or Screen Sharing to complete authentication.</td>",
    "</tr>",
    "<tr>",
    "<td>macOS blocks Node.js after install</td>",
    "<td>App Management security gate.</td>",
    "<td>System Settings &rarr; Privacy &amp; Security &rarr; App Management &rarr; Allow Node.js.</td>",
    "</tr>",
    "<tr>",
    "<td>Port conflict (address in use)</td>",
    "<td>Another service using port 5589.</td>",
    "<td>Stop the conflicting service, or change the port in <a href=\"#config/settings\">Configuration</a>.</td>",
    "</tr>",
    "</table>",
    "</div>",

    // Known Limitations.
    "<div class=\"section\">",
    "<h3>Known Limitations</h3>",
    "<ul>",
    "<li><strong>Bitrate is approximate.</strong> Chrome's media encoder treats the configured bitrate as a target, not a hard limit. ",
    "Actual bitrate may vary based on content complexity.</li>",
    "<li><strong>Frame rate follows the source.</strong> If the streaming site delivers 30fps, capture will be 30fps regardless of the configured ",
    "frame rate setting.</li>",
    "<li><strong>No closed captions.</strong> Chrome's capture API does not include caption data. Subtitles are not available in PrismCast streams.</li>",
    "<li><strong>No 4K, HDR, or surround sound.</strong> PrismCast captures H.264 video with AAC stereo audio. It is not a replacement for native ",
    "4K, HDR, Dolby Vision, or Dolby Atmos content.</li>",
    "<li><strong>Capture resolution is limited by display size.</strong> See the Display and Resolution section above for details.</li>",
    "<li><strong>Chrome may drop frames after extended use.</strong> The Chrome encoder can degrade after many hours of continuous operation. PrismCast ",
    "automatically restarts Chrome during idle periods to mitigate this.</li>",
    "</ul>",
    "</div>"
  ].join("\n");
}

/**
 * Generates the API Reference tab content with endpoint documentation.
 * @returns HTML content for the API Reference tab.
 */
function generateApiReferenceContent(): string {

  return [
    "<div class=\"section\">",
    "<p>PrismCast provides a RESTful HTTP API for streaming, management, and diagnostics.</p>",
    "</div>",

    // Streaming endpoints.
    "<div class=\"section\">",
    "<h3>Streaming</h3>",
    "<table>",
    "<tr><th style=\"width: 35%;\">Endpoint</th><th>Description</th></tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>GET /hls/:name/stream.m3u8</code></td>",
    "<td>HLS playlist for a named channel. Example: <code>/hls/nbc/stream.m3u8</code></td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>GET /hls/:name/init.mp4</code></td>",
    "<td>fMP4 initialization segment containing codec configuration.</td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>GET /hls/:name/:segment.m4s</code></td>",
    "<td>fMP4 media segment containing audio/video data.</td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>GET /play</code></td>",
    "<td>Stream any URL without creating a channel definition. Pass the URL as <code>?url=&lt;url&gt;</code>. " +
    "Advanced: <code>&amp;profile=</code> overrides auto-detection, <code>&amp;selector=</code> picks a channel on multi-channel sites, " +
    "<code>&amp;clickToPlay=true</code> clicks the video to start playback, <code>&amp;clickSelector=</code> specifies a play button element to click " +
    "(implies clickToPlay).</td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>GET /stream/:name</code></td>",
    "<td>MPEG-TS stream for HDHomeRun-compatible clients (e.g., Plex). Remuxes fMP4 to MPEG-TS with codec copy.</td>",
    "</tr>",
    "</table>",
    "</div>",

    // Playlist endpoints.
    "<div class=\"section\">",
    "<h3>Playlist</h3>",
    "<table>",
    "<tr><th style=\"width: 35%;\">Endpoint</th><th>Description</th></tr>",
    "<tr>",
    "<td class=\"endpoint\"><a href=\"/playlist\"><code>GET /playlist</code></a></td>",
    "<td>M3U playlist of all channels in Channels DVR format. Use this URL when adding PrismCast as a custom channel source. " +
    "Optional <code>?provider=</code> query parameter filters by streaming provider: " +
    "<code>?provider=yttv</code> (single), <code>?provider=yttv,sling</code> (multi-include), " +
    "<code>?provider=-hulu</code> (exclude). Tags are case-insensitive. " +
    "<strong>This only controls which channels appear in the playlist, not which provider is used for tuning.</strong></td>",
    "</tr>",
    "</table>",
    "</div>",

    // Management endpoints.
    "<div class=\"section\">",
    "<h3>Management</h3>",
    "<table>",
    "<tr><th style=\"width: 35%;\">Endpoint</th><th>Description</th></tr>",
    "<tr>",
    "<td class=\"endpoint\"><a href=\"/channels\"><code>GET /channels</code></a></td>",
    "<td>List all channels (predefined + user) as JSON with source, enabled status, and channel metadata.</td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><a href=\"/streams\"><code>GET /streams</code></a></td>",
    "<td>List all currently active streams with their ID, channel, URL, duration, and status.</td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>GET /streams/status</code></td>",
    "<td>Server-Sent Events stream for real-time stream and system status updates.</td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>DELETE /streams/:id</code></td>",
    "<td>Terminate a specific stream by its numeric ID. Returns 200 on success, 404 if not found.</td>",
    "</tr>",
    "</table>",
    "</div>",

    // Authentication endpoints.
    "<div class=\"section\">",
    "<h3>Authentication</h3>",
    "<table>",
    "<tr><th style=\"width: 35%;\">Endpoint</th><th>Description</th></tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>POST /auth/login</code></td>",
    "<td>Start login mode for a channel. Body: <code>{ \"channel\": \"name\" }</code> or <code>{ \"url\": \"...\" }</code></td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>POST /auth/done</code></td>",
    "<td>End login mode and close the login browser tab.</td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><a href=\"/auth/status\"><code>GET /auth/status</code></a></td>",
    "<td>Get current login status including whether login mode is active and which channel.</td>",
    "</tr>",
    "</table>",
    "</div>",

    // Configuration endpoints.
    "<div class=\"section\">",
    "<h3>Configuration</h3>",
    "<table>",
    "<tr><th style=\"width: 35%;\">Endpoint</th><th>Description</th></tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>POST /config</code></td>",
    "<td>Save configuration settings. Returns <code>{ success, message, willRestart, deferred, activeStreams }</code></td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><a href=\"/config/export\"><code>GET /config/export</code></a></td>",
    "<td>Export current configuration as a JSON file download.</td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>POST /config/import</code></td>",
    "<td>Import configuration from JSON. Server restarts to apply changes (if running as service).</td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>POST /config/restart-now</code></td>",
    "<td>Force immediate server restart regardless of active streams. Only works when running as a service.</td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>POST /config/channels</code></td>",
    "<td>Add, edit, or delete user channels. Body includes <code>action</code> (add/edit/delete) and channel data.</td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><a href=\"/config/channels/export\"><code>GET /config/channels/export</code></a></td>",
    "<td>Export user-defined channels as a JSON file download.</td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>POST /config/channels/import</code></td>",
    "<td>Import channels from JSON, replacing all existing user channels.</td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>POST /config/channels/import-m3u</code></td>",
    "<td>Import channels from M3U playlist. Body: <code>{ \"content\": \"...\", \"conflictMode\": \"skip\" | \"replace\" }</code></td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>POST /config/channels/toggle-predefined</code></td>",
    "<td>Enable or disable a single predefined channel. Body: <code>{ \"key\": \"nbc\", \"enabled\": true }</code></td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>POST /config/channels/toggle-all-predefined</code></td>",
    "<td>Enable or disable all predefined channels. Body: <code>{ \"enabled\": true }</code></td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>POST /config/provider</code></td>",
    "<td>Update provider selection for a multi-provider channel. Body: <code>{ \"channel\": \"nbc\", \"provider\": \"nbc-hulu\" }</code></td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>POST /config/provider-filter</code></td>",
    "<td>Set enabled provider tags. Body: <code>{ \"enabledProviders\": [\"hulu\", \"yttv\"] }</code>. Empty array disables filter.</td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>POST /config/provider-bulk-assign</code></td>",
    "<td>Assign a provider to all multi-provider channels. Body: <code>{ \"provider\": \"hulu\" }</code>. " +
    "Returns <code>{ affected, previousSelections, selections }</code></td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>POST /config/provider-bulk-restore</code></td>",
    "<td>Restore previous provider selections (undo bulk assign). Body: <code>{ \"selections\": { \"nbc\": \"nbc-hulu\", \"fox\": null } }</code>. " +
    "A <code>null</code> value restores the channel to its default provider.</td>",
    "</tr>",
    "</table>",
    "</div>",

    // Diagnostics endpoints.
    "<div class=\"section\">",
    "<h3>Diagnostics</h3>",
    "<table>",
    "<tr><th style=\"width: 35%;\">Endpoint</th><th>Description</th></tr>",
    "<tr>",
    "<td class=\"endpoint\"><a href=\"/health\"><code>GET /health</code></a></td>",
    "<td>Health check returning JSON with browser status, memory usage, stream counts, and configuration.</td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><a href=\"/logs\"><code>GET /logs</code></a></td>",
    "<td>Recent log entries as JSON. Query params: <code>?lines=N</code> (default 100, max 1000), <code>?level=error|warn|info</code></td>",
    "</tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>GET /logs/stream</code></td>",
    "<td>Server-Sent Events stream for real-time log entries. Query param: <code>?level=error|warn|info</code></td>",
    "</tr>",
    "</table>",
    "</div>",

    // Example responses.
    "<div class=\"section\">",
    "<h3>Example: Health Check Response</h3>",
    "<pre>{",
    "  \"browser\": { \"connected\": true, \"pageCount\": 2 },",
    "  \"captureMode\": \"ffmpeg\",",
    "  \"chrome\": \"Chrome/144.0.7559.110\",",
    "  \"clients\": { \"byType\": [{ \"count\": 1, \"type\": \"hls\" }], \"total\": 1 },",
    "  \"ffmpegAvailable\": true,",
    "  \"memory\": { \"heapTotal\": 120000000, \"heapUsed\": 85000000, \"rss\": 150000000, \"segmentBuffers\": 25000000 },",
    "  \"status\": \"healthy\",",
    "  \"streams\": { \"active\": 1, \"limit\": 10 },",
    "  \"timestamp\": \"2026-01-26T12:00:00.000Z\",",
    "  \"uptime\": 3600.5,",
    "  \"version\": \"1.0.12\"",
    "}</pre>",
    "</div>"
  ].join("\n");
}

/**
 * Generates the Channels tab content. This wraps the channels panel from config.ts and includes the login modal for channel authentication.
 * @returns HTML content for the Channels tab.
 */
function generateChannelsTabContent(): string {

  return [
    "<div class=\"section\">",
    generateChannelsPanel(),
    "</div>",

    // Login modal for channel authentication. Hidden by default, shown when user clicks "Login" on a channel.
    "<div id=\"login-modal\" class=\"login-modal\" style=\"display: none;\">",
    "<div class=\"login-modal-content\">",
    "<h3>Channel Authentication</h3>",
    "<p id=\"login-modal-message\">Complete authentication in the Chrome window on the PrismCast server, then click Done.</p>",
    "<p class=\"login-modal-hint\">A Chrome window has been opened on the machine running PrismCast. ",
    "If PrismCast is running on a remote server or headless system, you'll need screen sharing ",
    "(VNC, Screen Sharing, etc.) to access it. Sign in with your TV provider credentials in that window. ",
    "This login session will automatically close after 15 minutes.</p>",
    "<div class=\"login-modal-buttons\">",
    "<button type=\"button\" class=\"btn btn-primary\" onclick=\"endLogin()\">Done</button>",
    "</div>",
    "</div>",
    "</div>"
  ].join("\n");
}

/**
 * Generates the Logs tab content with the log viewer controls and display area. Uses Server-Sent Events for real-time log streaming instead of polling.
 * @returns HTML content for the Logs tab.
 */
function generateLogsContent(): string {

  return [
    "<div class=\"section\">",
    "<div class=\"log-controls\" style=\"display: flex; gap: 15px; align-items: center; margin-bottom: 15px; flex-wrap: wrap;\">",
    "<div>",
    "<label for=\"log-level\" style=\"margin-right: 5px;\">Level:</label>",
    "<select id=\"log-level\" onchange=\"onLevelChange()\">",
    "<option value=\"\">All</option>",
    "<option value=\"error\">Errors</option>",
    "<option value=\"warn\">Warnings</option>",
    "<option value=\"info\">Info</option>",
    "</select>",
    "</div>",
    "<button class=\"btn btn-primary btn-sm\" onclick=\"loadLogs()\">Reload History</button>",
    "<span id=\"sse-status\" style=\"font-size: 13px; margin-left: auto;\"></span>",
    "</div>",
    "</div>",
    "<div id=\"log-container\" class=\"log-viewer\">",
    "<div class=\"log-connecting\">Connecting...</div>",
    "</div>",

    // Log viewer JavaScript with SSE support.
    "<script>",
    "var logContainer = document.getElementById('log-container');",
    "var sseStatus = document.getElementById('sse-status');",
    "var eventSource = null;",
    "var isConsoleMode = false;",
    "var currentLevel = '';",

    // Load historical logs from the /logs endpoint.
    "function loadLogs() {",
    "  var level = document.getElementById('log-level').value;",
    "  var url = '/logs?lines=500';",
    "  if(level) { url += '&level=' + level; }",
    "  fetch(url)",
    "    .then(function(res) { return res.json(); })",
    "    .then(function(data) {",
    "      if(data.mode === 'console') {",
    "        isConsoleMode = true;",
    "        logContainer.innerHTML = '<div class=\"log-warn\">File logging is disabled. Logs are being written to the console.</div>';",
    "        return;",
    "      }",
    "      isConsoleMode = false;",
    "      if(data.entries.length === 0) {",
    "        logContainer.innerHTML = '<div class=\"log-muted\">No log entries found.</div>';",
    "      } else {",
    "        renderHistoricalLogs(data.entries);",
    "      }",
    "    })",
    "    .catch(function(err) {",
    "      logContainer.innerHTML = '<div class=\"log-error\">Error loading logs: ' + err.message + '</div>';",
    "    });",
    "}",

    // Render historical log entries (replaces container content).
    "function renderHistoricalLogs(entries) {",
    "  var html = '';",
    "  for (var i = 0; i < entries.length; i++) {",
    "    html += formatLogEntry(entries[i]);",
    "  }",
    "  logContainer.innerHTML = html;",
    "  logContainer.scrollTop = logContainer.scrollHeight;",
    "}",

    // Format a single log entry as HTML using CSS classes for theme-aware colors.
    "function formatLogEntry(entry) {",
    "  var cls = 'log-entry';",
    "  if(entry.level === 'error') { cls += ' log-error'; }",
    "  else if(entry.level === 'warn') { cls += ' log-warn'; }",
    "  else if(entry.level === 'debug') { cls += ' log-debug'; }",
    "  var levelBadge = '';",
    "  if(entry.level !== 'info') {",
    "    var tag = entry.categoryTag ? entry.level.toUpperCase() + ':' + entry.categoryTag : entry.level.toUpperCase();",
    "    levelBadge = '[' + escapeHtml(tag) + '] ';",
    "  }",
    "  return '<div class=\"' + cls + '\">[' + escapeHtml(entry.timestamp) + '] ' + levelBadge + escapeHtml(entry.message) + '</div>';",
    "}",

    // Append a single log entry (for SSE streaming).
    "function appendLogEntry(entry) {",
    "  if (isConsoleMode) { return; }",
    "  var level = document.getElementById('log-level').value;",
    "  if (level && (entry.level !== level)) { return; }",
    "  var wasAtBottom = (logContainer.scrollHeight - logContainer.scrollTop - logContainer.clientHeight) < 50;",
    "  var entryHtml = formatLogEntry(entry);",
    "  logContainer.insertAdjacentHTML('beforeend', entryHtml);",
    "  if (wasAtBottom) { logContainer.scrollTop = logContainer.scrollHeight; }",
    "}",

    "function escapeHtml(text) {",
    "  var div = document.createElement('div');",
    "  div.textContent = text;",
    "  return div.innerHTML;",
    "}",

    // Track the last time any SSE event was received from the logs stream. Used by the staleness checker below.
    "var lastLogsEventTime = 0;",
    "var logsStalenessInterval = null;",

    // Connect to the SSE stream.
    "function connectSSE() {",
    "  if(eventSource) { eventSource.close(); }",
    "  if(logsStalenessInterval) { clearInterval(logsStalenessInterval); }",
    "  eventSource = new EventSource('/logs/stream');",
    "  lastLogsEventTime = Date.now();",
    "  sseStatus.innerHTML = '<span class=\"status-dot\" style=\"color: var(--stream-buffering);\">&#9679;</span> Connecting...';",

    // Same on() wrapper pattern as the status stream. Updates the staleness timestamp on every data event so the 45-second checker stays
    // satisfied as long as any data (heartbeats or log entries) is flowing. Lifecycle handlers (onopen, onerror) stay outside the wrapper.
    "  function on(event, handler) {",
    "    eventSource.addEventListener(event, function(e) {",
    "      lastLogsEventTime = Date.now();",
    "      if(handler) { handler(e); }",
    "    });",
    "  }",
    "  on('heartbeat');",
    "  on('message', function(e) {",
    "    try {",
    "      var entry = JSON.parse(e.data);",
    "      appendLogEntry(entry);",
    "    } catch(err) { /* Ignore parse errors. */ }",
    "  });",
    "  eventSource.onopen = function() {",
    "    lastLogsEventTime = Date.now();",
    "    sseStatus.innerHTML = '<span class=\"status-dot\" style=\"color: var(--stream-healthy);\">&#9679;</span> Live';",
    "    loadLogs();",
    "  };",
    "  eventSource.onerror = function() {",
    "    sseStatus.innerHTML = '<span class=\"status-dot\" style=\"color: var(--stream-error);\">&#9679;</span> Disconnected';",
    "  };",
    "  logsStalenessInterval = setInterval(function() {",
    "    if((Date.now() - lastLogsEventTime) > 45000) { connectSSE(); }",
    "  }, 45000);",
    "}",

    // Disconnect from the SSE stream.
    "function disconnectSSE() {",
    "  if(logsStalenessInterval) { clearInterval(logsStalenessInterval); logsStalenessInterval = null; }",
    "  if (eventSource) {",
    "    eventSource.close();",
    "    eventSource = null;",
    "  }",
    "  sseStatus.innerHTML = '';",
    "}",

    // Handle level filter change (reload history with new filter, SSE filters client-side).
    "function onLevelChange() {",
    "  loadLogs();",
    "}",

    // Handle tab activation events for logs SSE connection. The onopen handler calls loadLogs() to ensure history is loaded on both initial
    // connection and reconnection after a disconnect.
    "document.addEventListener('tabactivated', function(e) {",
    "  if (e.detail.category === 'logs') {",
    "    connectSSE();",
    "  } else {",
    "    disconnectSSE();",
    "  }",
    "});",

    "</script>"
  ].join("\n");
}

/**
 * Generates the Backup subtab content with download and import functionality for both settings and channels.
 * @returns HTML content for the Backup subtab panel.
 */
function generateBackupPanel(): string {

  // Description text varies based on whether running as a managed service.
  const restartDescription = isRunningAsService() ?
    "The server will restart automatically to apply the imported settings." :
    "After importing, you will need to restart PrismCast for changes to take effect.";

  return [

    // Panel description.
    "<p class=\"settings-panel-description\">Export and import configuration and channel data.</p>",

    // Settings backup section.
    "<div class=\"backup-group\">",
    "<div class=\"backup-group-title\">Settings Backup</div>",
    "<div class=\"backup-section\">",
    "<h3>Download Settings</h3>",
    "<p>Download your current server configuration as a JSON file. This includes all settings (server, browser, streaming, playback, etc.) ",
    "but does not include channel definitions.</p>",
    "<button type=\"button\" class=\"btn btn-export\" onclick=\"exportConfig()\">Download Settings</button>",
    "</div>",
    "<div class=\"backup-section\">",
    "<h3>Import Settings</h3>",
    "<p>Import a previously saved settings file. " + restartDescription + "</p>",
    "<button type=\"button\" class=\"btn btn-import\" onclick=\"document.getElementById('import-settings-file').click()\">Import Settings</button>",
    "<input type=\"file\" id=\"import-settings-file\" accept=\".json\" onchange=\"importConfig(this)\">",
    "</div>",
    "</div>",

    // Channels backup section.
    "<div class=\"backup-group\">",
    "<div class=\"backup-group-title\">Channels Backup</div>",
    "<div class=\"backup-section\">",
    "<h3>Download Channels</h3>",
    "<p>Download your custom channel definitions as a JSON file. This includes only user-defined channels, not the predefined channels ",
    "built into PrismCast.</p>",
    "<button type=\"button\" class=\"btn btn-export\" onclick=\"exportChannels()\">Download Channels</button>",
    "</div>",
    "<div class=\"backup-section\">",
    "<h3>Import Channels</h3>",
    "<p>Import channel definitions from a previously saved file. This will <strong>replace all existing user channels</strong>.</p>",
    "<button type=\"button\" class=\"btn btn-import\" onclick=\"document.getElementById('import-channels-file').click()\">Import Channels</button>",
    "<input type=\"file\" id=\"import-channels-file\" accept=\".json\" onchange=\"importChannels(this)\">",
    "</div>",
    "</div>"
  ].join("\n");
}

/**
 * Generates the Configuration tab content with subtabs for channels, settings, advanced, and backup.
 * @returns HTML content for the Configuration tab.
 */
function generateConfigContent(): string {

  const tabs = getUITabs();
  const lines: string[] = [];

  // Environment variable warning if applicable.
  if(hasEnvOverrides()) {

    lines.push("<div class=\"warning\">");
    lines.push("<div class=\"warning-title\">Environment Variable Overrides</div>");
    lines.push("Some settings are overridden by environment variables and cannot be changed through this interface. ");
    lines.push("To modify these settings, update your environment variables and restart the server.");
    lines.push("</div>");
  }

  // Subtab bar: Settings tabs plus Backup.
  lines.push("<div class=\"subtab-bar\" role=\"tablist\">");

  let isFirst = true;

  for(const tab of tabs) {

    const activeClass = isFirst ? " active" : "";
    const ariaSelected = isFirst ? "true" : "false";

    lines.push("<button type=\"button\" class=\"subtab-btn" + activeClass + "\" data-subtab=\"" + escapeHtml(tab.id) + "\" role=\"tab\" aria-selected=\"" +
      ariaSelected + "\">" + escapeHtml(tab.displayName) + "</button>");
    isFirst = false;
  }

  lines.push("<button type=\"button\" class=\"subtab-btn\" data-subtab=\"backup\" role=\"tab\" aria-selected=\"false\">Backup</button>");
  lines.push("</div>");

  // Start the settings form (wraps settings and advanced subtabs, not channels or backup).
  lines.push("<form id=\"settings-form\" onsubmit=\"return submitSettingsForm(event)\">");

  // Settings subtab panel with non-collapsible section headers (default active subtab).
  lines.push("<div id=\"subtab-settings\" class=\"subtab-panel active\" role=\"tabpanel\">");
  lines.push(generateSettingsTabContent());
  lines.push("</div>");

  // Advanced subtab panel with collapsible sections.
  lines.push("<div id=\"subtab-advanced\" class=\"subtab-panel\" role=\"tabpanel\">");
  lines.push(generateAdvancedTabContent());
  lines.push("</div>");

  // Settings buttons (hidden on Backup subtab). Button text varies based on whether running as a managed service.
  const saveButtonText = isRunningAsService() ? "Save &amp; Restart" : "Save Settings";

  lines.push("<div id=\"settings-buttons\" class=\"button-row\" style=\"display: flex;\">");
  lines.push("<button type=\"submit\" class=\"btn btn-primary\" id=\"save-btn\">" + saveButtonText + "</button>");
  lines.push("<button type=\"button\" class=\"btn btn-danger\" onclick=\"resetAllToDefaults()\">Reset All to Defaults</button>");
  lines.push("</div>");

  lines.push("</form>");

  // Backup subtab panel (outside form since it doesn't contain settings inputs).
  lines.push("<div id=\"subtab-backup\" class=\"subtab-panel\" role=\"tabpanel\">");
  lines.push(generateBackupPanel());
  lines.push("</div>");

  // Config path display.
  lines.push(generateSettingsFormFooter());

  return lines.join("\n");
}

/**
 * Generates the JavaScript for config subtab switching, channel editing, presets, validation, and import/export functionality.
 * @returns JavaScript code as a string wrapped in script tags.
 */
function generateConfigSubtabScript(): string {

  // Build preset data for auto-filling bitrate and frame rate when preset changes. Viewport is derived server-side and not included here.
  const presetBlocks: string[] = [];

  for(const preset of VIDEO_QUALITY_PRESETS) {

    // Only include bitrate and frame rate, not viewport (viewport is derived from preset server-side).
    const bitrateValue = preset.values["streaming.videoBitsPerSecond"];
    const frameRateValue = preset.values["streaming.frameRate"];

    // Convert bitrate from bps to Mbps for display.
    const block = [
      "    '" + preset.id + "': {",
      "      'streaming-videoBitsPerSecond': " + String(bitrateValue / 1000000) + ",",
      "      'streaming-frameRate': " + String(frameRateValue),
      "    }"
    ].join("\n");

    presetBlocks.push(block);
  }

  // Pass service status to JavaScript for conditional messaging.
  const isService = isRunningAsService();

  return [
    "<script>",
    "(function() {",

    // Service mode flag for conditional UI behavior.
    "  var isServiceMode = " + String(isService) + ";",

    // Preset values for auto-filling bitrate and frame rate.
    "  var presetValues = {",
    presetBlocks.join(",\n"),
    "  };",

    // When quality preset changes, auto-fill bitrate and frame rate with preset values.
    "  function onPresetChange(presetId) {",
    "    var values = presetValues[presetId];",
    "    if (!values) return;",
    "    for (var inputId in values) {",
    "      var input = document.getElementById(inputId);",
    "      if (input) {",
    "        input.value = values[inputId];",
    "        input.dispatchEvent(new Event('input', { bubbles: true }));",
    "      }",
    "    }",
    "  }",

    // Show a toast notification. Auto-dismiss durations: success/info = 5s, warning = 8s, error = no auto-dismiss. Optional action: { label, onclick } appends an
    // inline button between the message text and the close button.
    "  function showToast(message, type, duration, action) {",
    "    var container = document.getElementById('toast-container');",
    "    if (!container) return;",
    "    var toast = document.createElement('div');",
    "    toast.className = 'toast ' + (type || 'info');",
    "    toast.textContent = message;",
    "    toast.setAttribute('role', (type === 'error' || type === 'warning') ? 'alert' : 'status');",
    "    if (action && action.label) {",
    "      var actionBtn = document.createElement('button');",
    "      actionBtn.type = 'button';",
    "      actionBtn.className = 'toast-action';",
    "      actionBtn.textContent = action.label;",
    "      actionBtn.onclick = function() { if (action.onclick) action.onclick(); dismissToast(toast); };",
    "      toast.appendChild(actionBtn);",
    "    }",
    "    var closeBtn = document.createElement('button');",
    "    closeBtn.type = 'button';",
    "    closeBtn.className = 'toast-close';",
    "    closeBtn.textContent = '\\u00d7';",
    "    closeBtn.onclick = function() { dismissToast(toast); };",
    "    toast.appendChild(closeBtn);",
    "    container.appendChild(toast);",
    "    var ms = duration !== undefined ? duration : type === 'error' ? 0 : type === 'warning' ? 8000 : 5000;",
    "    if (ms > 0) { setTimeout(function() { dismissToast(toast); }, ms); }",
    "  }",

    // Dismiss a toast with slide-out animation.
    "  function dismissToast(toast) {",
    "    if (toast.classList.contains('toast-exit')) return;",
    "    toast.classList.add('toast-exit');",
    "    toast.addEventListener('animationend', function() { if (toast.parentNode) toast.parentNode.removeChild(toast); });",
    "  }",

    // Queue a toast to appear after the next page reload.
    "  function showToastAfterReload(message, type) {",
    "    sessionStorage.setItem('pendingToast', JSON.stringify({ message: message, type: type || 'success' }));",
    "    location.reload();",
    "  }",

    // Interval handle for restart polling.
    "  var restartPollInterval = null;",

    // Track whether a restart is pending (deferred due to active streams).
    "  var pendingRestart = false;",

    // Show the pending restart dialog when streams are active.
    "  function showPendingRestartDialog(streamCount) {",
    "    pendingRestart = true;",
    "    document.getElementById('restart-stream-count').textContent = streamCount;",
    "    document.getElementById('restart-dialog').style.display = 'flex';",
    "    updateRestartDialogStatus();",
    "  }",

    // Update the restart dialog when stream count changes.
    "  function updateRestartDialogStatus() {",
    "    var count = Object.keys(streamData).length;",
    "    document.getElementById('restart-stream-count').textContent = count;",
    "    if (count === 0 && pendingRestart) {",
    "      pendingRestart = false;",
    "      document.getElementById('restart-dialog').style.display = 'none';",
    "      triggerRestart();",
    "    }",
    "  }",

    // Cancel the pending restart.
    "  window.cancelPendingRestart = function() {",
    "    pendingRestart = false;",
    "    document.getElementById('restart-dialog').style.display = 'none';",
    "    showToast('Restart cancelled. Changes will apply on next restart.', 'info');",
    "  };",

    // Force immediate restart despite active streams.
    "  window.forceRestart = function() {",
    "    pendingRestart = false;",
    "    document.getElementById('restart-dialog').style.display = 'none';",
    "    fetch('/config/restart-now', { method: 'POST' })",
    "      .then(function(response) {",
    "        if (response.ok) {",
    "          waitForServerRestart();",
    "        } else {",
    "          return response.json().then(function(data) {",
    "            throw new Error(data.message || 'Restart failed');",
    "          });",
    "        }",
    "      })",
    "      .catch(function(err) {",
    "        showToast('Failed to restart: ' + err.message, 'error');",
    "      });",
    "  };",

    // Trigger restart (called when streams reach 0).
    "  function triggerRestart() {",
    "    fetch('/config/restart-now', { method: 'POST' })",
    "      .then(function(response) {",
    "        if (response.ok) {",
    "          waitForServerRestart();",
    "        }",
    "      })",
    "      .catch(function() {",
    "        showToast('Failed to trigger restart. Please restart manually.', 'error');",
    "      });",
    "  }",

    // Wait for server restart by polling /health, then reload.
    "  function waitForServerRestart() {",
    "    var attempts = 0;",
    "    var maxAttempts = 30;",
    "    showToast('Restarting server...', 'info', 0);",
    "    if (restartPollInterval) { clearInterval(restartPollInterval); }",
    "    restartPollInterval = setInterval(function() {",
    "      attempts++;",
    "      fetch('/health')",
    "        .then(function(response) {",
    "          if (response.ok) {",
    "            clearInterval(restartPollInterval);",
    "            restartPollInterval = null;",
    "            showToastAfterReload('Server restarted.', 'success');",
    "          }",
    "        })",
    "        .catch(function() {",
    "          if (attempts >= maxAttempts) {",
    "            clearInterval(restartPollInterval);",
    "            restartPollInterval = null;",
    "            showToast('Server did not restart within 30 seconds. Please check the server manually.', 'error');",
    "          }",
    "        });",
    "    }, 1000);",
    "  }",

    // Escape HTML entities in text for safe display.
    "  function escapeHtmlText(text) {",
    "    var div = document.createElement('div');",
    "    div.textContent = text;",
    "    return div.innerHTML;",
    "  }",

    // Open the changelog modal and fetch content dynamically. Also checks whether an upgrade button should be shown.
    "  window.openChangelogModal = function() {",
    "    var modal = document.getElementById('changelog-modal');",
    "    if (!modal) return;",
    "    var title = modal.querySelector('.changelog-title');",
    "    var loading = modal.querySelector('.changelog-loading');",
    "    var content = modal.querySelector('.changelog-content');",
    "    var error = modal.querySelector('.changelog-error');",
    "    var upgradeBtn = document.getElementById('changelog-upgrade-btn');",
    "    modal.style.display = 'flex';",
    "    loading.style.display = 'block';",
    "    content.style.display = 'none';",
    "    error.style.display = 'none';",
    "    if (upgradeBtn) upgradeBtn.style.display = 'none';",
    "    fetch('/version/changelog')",
    "      .then(function(res) { return res.json(); })",
    "      .then(function(data) {",
    "        loading.style.display = 'none';",
    "        title.textContent = \"What's new in v\" + data.displayVersion;",
    "        if (data.items && data.items.length > 0) {",
    "          var html = '<ul class=\"changelog-list\">';",
    "          for (var i = 0; i < data.items.length; i++) {",
    "            html += '<li>' + escapeHtmlText(data.items[i]) + '</li>';",
    "          }",
    "          html += '</ul>';",
    "          content.innerHTML = html;",
    "          content.style.display = 'block';",
    "        } else {",
    "          error.style.display = 'block';",
    "        }",
    "        if (data.updateAvailable && upgradeBtn) { upgradeBtn.style.display = ''; }",
    "      })",
    "      .catch(function() {",
    "        loading.style.display = 'none';",
    "        error.style.display = 'block';",
    "      });",
    "  };",

    // Close the changelog modal.
    "  window.closeChangelogModal = function() {",
    "    var modal = document.getElementById('changelog-modal');",
    "    if (modal) { modal.style.display = 'none'; }",
    "  };",

    // Start the upgrade process from the changelog modal. Fetches upgrade info, confirms with the user if there are active streams, then executes the upgrade.
    "  window.startUpgrade = function() {",
    "    fetch('/upgrade/info')",
    "      .then(function(res) { return res.json(); })",
    "      .then(function(info) {",
    "        if (!info.upgradeable) {",
    "          closeChangelogModal();",
    "          var msg = info.method === 'docker'",
    "            ? 'Docker containers cannot be upgraded in-place. Pull the latest image and recreate the container.'",
    "            : 'Manual upgrade required: ' + info.upgradeCommand;",
    "          showToast(msg, 'info', 8000);",
    "          return;",
    "        }",
    "        var streamCount = Object.keys(streamData).length;",
    "        if (streamCount > 0) {",
    "          if (!confirm('There are ' + streamCount + ' active stream(s). Upgrading will interrupt them. Continue?')) return;",
    "        }",
    "        closeChangelogModal();",
    "        showToast('Upgrading PrismCast...', 'info', 0);",
    "        return fetch('/upgrade', { method: 'POST' })",
    "          .then(function(res) { return res.json(); })",
    "          .then(function(result) {",
    "            if (result.success && result.willRestart) {",
    "              waitForServerRestart();",
    "            } else if (result.success) {",
    "              showToast('Upgrade complete. Please restart PrismCast manually.', 'success', 8000);",
    "            } else {",
    "              showToast('Upgrade failed: ' + result.message, 'error');",
    "            }",
    "          });",
    "      })",
    "      .catch(function(err) {",
    "        showToast('Upgrade failed: ' + err.message, 'error');",
    "      });",
    "  };",

    // Check for updates manually. Updates the version link in-place when a new version is found.
    "  window.checkForUpdates = function() {",
    "    var btn = document.querySelector('.version-check');",
    "    if (!btn || btn.classList.contains('checking')) return;",
    "    btn.classList.add('checking');",
    "    fetch('/version/check', { method: 'POST' })",
    "      .then(function(res) { return res.json(); })",
    "      .then(function(data) {",
    "        btn.classList.remove('checking');",
    "        if (data.updateAvailable && data.latestVersion) {",
    "          var link = document.querySelector('.version-container .version');",
    "          if (link && !link.classList.contains('version-update')) {",
    "            link.textContent = 'v' + data.currentVersion + ' \\u2192 v' + data.latestVersion;",
    "            link.classList.add('version-update');",
    "          }",
    "        } else {",
    "          btn.classList.add('up-to-date');",
    "          setTimeout(function() { btn.classList.remove('up-to-date'); }, 2000);",
    "        }",
    "      })",
    "      .catch(function() {",
    "        btn.classList.remove('checking');",
    "        btn.classList.add('check-error');",
    "        setTimeout(function() { btn.classList.remove('check-error'); }, 2000);",
    "      });",
    "  };",

    // Clear all field error indicators.
    "  function clearFieldErrors() {",
    "    var errorInputs = document.querySelectorAll('.form-input.error, .form-select.error');",
    "    for (var i = 0; i < errorInputs.length; i++) {",
    "      errorInputs[i].classList.remove('error');",
    "    }",
    "    var errorMsgs = document.querySelectorAll('.form-error.dynamic');",
    "    for (var j = 0; j < errorMsgs.length; j++) {",
    "      errorMsgs[j].remove();",
    "    }",
    "  }",

    // Display field-level errors from server response.
    "  function displayFieldErrors(errors) {",
    "    for (var path in errors) {",
    "      var inputId = path.replace(/\\./g, '-');",
    "      var input = document.getElementById(inputId);",
    "      if (input) {",
    "        input.classList.add('error');",
    "        var errorDiv = document.createElement('div');",
    "        errorDiv.className = 'form-error dynamic';",
    "        errorDiv.textContent = errors[path];",
    "        input.closest('.form-group').appendChild(errorDiv);",
    "      }",
    "    }",
    "  }",

    // Set a form input's value, handling checkbox and standard input types uniformly.
    "  function setInputValue(input, value) {",
    "    if (input.type === 'checkbox') {",
    "      input.checked = value === 'true';",
    "    } else {",
    "      input.value = value;",
    "    }",
    "  }",

    // Get a form input's current value as a string, handling checkbox and standard input types uniformly.
    "  function getInputValue(input) {",
    "    return input.type === 'checkbox' ? String(input.checked) : input.value;",
    "  }",

    // Toggle dependent fields when a boolean checkbox changes. Fields with data-depends-on are visually greyed out and removed from the tab order when the
    // referenced checkbox is unchecked. This function is defined at the top level so that both event handlers and reset functions can call it.
    "  function updateDependentFields(checkboxId) {",
    "    var checkbox = document.getElementById(checkboxId);",
    "    if (!checkbox) return;",
    "    var dependents = document.querySelectorAll('[data-depends-on=\"' + checkboxId + '\"]');",
    "    for (var i = 0; i < dependents.length; i++) {",
    "      if (checkbox.checked) {",
    "        dependents[i].classList.remove('depends-disabled');",
    "      } else {",
    "        dependents[i].classList.add('depends-disabled');",
    "      }",
    "      var depInputs = dependents[i].querySelectorAll('input:not([type=\"hidden\"]), select');",
    "      for (var j = 0; j < depInputs.length; j++) {",
    "        depInputs[j].tabIndex = checkbox.checked ? 0 : -1;",
    "      }",
    "    }",
    "  }",

    // Update modified indicators for a single input.
    "  function updateModifiedIndicator(input) {",
    "    var defaultVal = input.getAttribute('data-default');",
    "    var currentVal = getInputValue(input);",
    "    var formGroup = input.closest('.form-group');",
    "    if (!formGroup) return;",
    "    var isModified = currentVal !== defaultVal;",
    "    var dot = formGroup.querySelector('.modified-dot');",
    "    var resetBtn = formGroup.querySelector('.btn-reset');",
    "    if (isModified) {",
    "      formGroup.classList.add('modified');",
    "      if (!dot) {",
    "        var label = formGroup.querySelector('.form-label');",
    "        if (label) {",
    "          var newDot = document.createElement('span');",
    "          newDot.className = 'modified-dot';",
    "          newDot.title = 'Modified from default';",
    "          label.insertBefore(newDot, label.firstChild);",
    "        }",
    "      }",
    "      if (!resetBtn) {",
    "        var row = formGroup.querySelector('.form-row');",
    "        if (row) {",
    "          var path = input.getAttribute('name');",
    "          var btn = document.createElement('button');",
    "          btn.type = 'button';",
    "          btn.className = 'btn-reset';",
    "          btn.title = 'Reset to default';",
    "          btn.innerHTML = '&#8635;';",
    "          btn.onclick = function() { resetSetting(path); };",
    "          row.appendChild(btn);",
    "        }",
    "      }",
    "    } else {",
    "      formGroup.classList.remove('modified');",
    "      if (dot) dot.remove();",
    "      if (resetBtn) resetBtn.remove();",
    "    }",
    "  }",

    // Reset a single setting to its default value (client-side only). Dispatches both input and change events to match browser behavior: input for validation and
    // modified indicator updates, change for cascade handlers (e.g., preset dropdown updating bitrate and frame rate fields).
    "  window.resetSetting = function(path) {",
    "    var inputId = path.replace(/\\./g, '-');",
    "    var input = document.getElementById(inputId);",
    "    if (!input) return;",
    "    var defaultVal = input.getAttribute('data-default');",
    "    if (defaultVal !== null) {",
    "      setInputValue(input, defaultVal);",
    "      input.dispatchEvent(new Event('input', { bubbles: true }));",
    "      input.dispatchEvent(new Event('change', { bubbles: true }));",
    "    }",
    "  };",

    // Reset all settings in a tab to defaults (client-side only). Works for both settings and advanced tabs.
    "  window.resetTabToDefaults = function(tabId) {",
    "    if (!confirm('Reset all settings in this tab to defaults?')) return;",
    "    var panel = document.getElementById('subtab-' + tabId);",
    "    if (!panel) return;",
    "    var inputs = panel.querySelectorAll('input[data-default], select[data-default]');",
    "    for (var i = 0; i < inputs.length; i++) {",
    "      var input = inputs[i];",
    "      if (!input.disabled) {",
    "        setInputValue(input, input.getAttribute('data-default'));",
    "        updateModifiedIndicator(input);",
    "      }",
    "    }",
    "    var cbInputs = panel.querySelectorAll('input[type=\"checkbox\"][data-default]');",
    "    for (var j = 0; j < cbInputs.length; j++) {",
    "      if (!cbInputs[j].disabled) {",
    "        updateDependentFields(cbInputs[j].id);",
    "      }",
    "    }",
    "    showToast('Settings reset to defaults. Click ' + (isServiceMode ? 'Save & Restart' : 'Save Settings') + ' to apply changes.', 'info');",
    "  };",

    // Toggle collapsible section in Advanced tab.
    "  window.toggleSection = function(sectionId) {",
    "    var section = document.querySelector('.advanced-section[data-section=\"' + sectionId + '\"]');",
    "    if (!section) return;",
    "    var header = section.querySelector('.section-header');",
    "    var content = section.querySelector('.section-content');",
    "    if (!header || !content) return;",
    "    var isExpanded = content.classList.contains('expanded');",
    "    if (isExpanded) {",
    "      content.classList.remove('expanded');",
    "      header.classList.remove('expanded');",
    "    } else {",
    "      content.classList.add('expanded');",
    "      header.classList.add('expanded');",
    "    }",
    "    try {",
    "      var expanded = JSON.parse(localStorage.getItem('prismcast-advanced-sections') || '{}');",
    "      expanded[sectionId] = !isExpanded;",
    "      localStorage.setItem('prismcast-advanced-sections', JSON.stringify(expanded));",
    "    } catch(e) {}",
    "  };",

    // Initialize section expansion state from localStorage.
    "  function initSections() {",
    "    try {",
    "      var expanded = JSON.parse(localStorage.getItem('prismcast-advanced-sections') || '{}');",
    "      for (var sectionId in expanded) {",
    "        if (expanded[sectionId]) {",
    "          var section = document.querySelector('.advanced-section[data-section=\"' + sectionId + '\"]');",
    "          if (section) {",
    "            var header = section.querySelector('.section-header');",
    "            var content = section.querySelector('.section-content');",
    "            if (header && content) {",
    "              header.classList.add('expanded');",
    "              content.classList.add('expanded');",
    "            }",
    "          }",
    "        }",
    "      }",
    "    } catch(e) {}",
    "  }",
    "  initSections();",

    // Reset all settings to defaults (client-side only).
    "  window.resetAllToDefaults = function() {",
    "    if (!confirm('Reset ALL settings to defaults? Click ' + (isServiceMode ? 'Save & Restart' : 'Save Settings') + ' after to apply.')) return;",
    "    var form = document.getElementById('settings-form');",
    "    if (!form) return;",
    "    var inputs = form.querySelectorAll('input[data-default], select[data-default]');",
    "    for (var i = 0; i < inputs.length; i++) {",
    "      var input = inputs[i];",
    "      if (!input.disabled) {",
    "        setInputValue(input, input.getAttribute('data-default'));",
    "        updateModifiedIndicator(input);",
    "      }",
    "    }",
    "    var cbInputs = form.querySelectorAll('input[type=\"checkbox\"][data-default]');",
    "    for (var j = 0; j < cbInputs.length; j++) {",
    "      if (!cbInputs[j].disabled) {",
    "        updateDependentFields(cbInputs[j].id);",
    "      }",
    "    }",
    "    showToast('All settings reset to defaults. Click ' + (isServiceMode ? 'Save & Restart' : 'Save Settings') + ' to apply changes.', 'info');",
    "  };",

    // Submit settings form via AJAX.
    "  window.submitSettingsForm = function(event) {",
    "    event.preventDefault();",
    "    clearFieldErrors();",
    "    var form = document.getElementById('settings-form');",
    "    var saveBtn = document.getElementById('save-btn');",
    "    if (!form) return false;",
    "    var formData = new FormData(form);",
    "    var config = {};",
    "    for (var pair of formData.entries()) {",
    "      var path = pair[0];",
    "      var value = pair[1];",
    "      var parts = path.split('.');",
    "      var obj = config;",
    "      for (var i = 0; i < parts.length - 1; i++) {",
    "        if (!obj[parts[i]]) obj[parts[i]] = {};",
    "        obj = obj[parts[i]];",
    "      }",
    "      obj[parts[parts.length - 1]] = value;",
    "    }",
    "    if (saveBtn) saveBtn.classList.add('loading');",
    "    fetch('/config', {",
    "      method: 'POST',",
    "      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },",
    "      body: JSON.stringify(config)",
    "    })",
    "    .then(function(response) { return response.json().then(function(data) { return { ok: response.ok, data: data }; }); })",
    "    .then(function(result) {",
    "      if (saveBtn) saveBtn.classList.remove('loading');",
    "      if (result.ok && result.data.success) {",
    "        if (result.data.willRestart) {",
    "          if (result.data.deferred) {",
    "            showPendingRestartDialog(result.data.activeStreams);",
    "          } else {",
    "            waitForServerRestart();",
    "          }",
    "        } else {",
    "          showToast(result.data.message || 'Configuration saved.', 'info');",
    "        }",
    "      } else if (result.data.errors) {",
    "        displayFieldErrors(result.data.errors);",
    "        showToast('Please correct the errors below.', 'error');",
    "      } else {",
    "        showToast(result.data.message || 'Failed to save configuration.', 'error');",
    "      }",
    "    })",
    "    .catch(function(err) {",
    "      if (saveBtn) saveBtn.classList.remove('loading');",
    "      showToast('Failed to save configuration: ' + err.message, 'error');",
    "    });",
    "    return false;",
    "  };",

    // Export configuration as JSON download.
    "  window.exportConfig = function() {",
    "    fetch('/config/export')",
    "      .then(function(response) { return response.blob(); })",
    "      .then(function(blob) {",
    "        var url = window.URL.createObjectURL(blob);",
    "        var a = document.createElement('a');",
    "        a.href = url;",
    "        a.download = 'prismcast-config.json';",
    "        document.body.appendChild(a);",
    "        a.click();",
    "        document.body.removeChild(a);",
    "        window.URL.revokeObjectURL(url);",
    "      })",
    "      .catch(function(err) { showToast('Failed to export configuration: ' + err.message, 'error'); });",
    "  };",

    // Import configuration from file.
    "  window.importConfig = function(fileInput) {",
    "    var file = fileInput.files[0];",
    "    if (!file) return;",
    "    var reader = new FileReader();",
    "    reader.onload = function(e) {",
    "      try {",
    "        var config = JSON.parse(e.target.result);",
    "        if (confirm('Import this configuration? The server may restart to apply changes.')) {",
    "          fetch('/config/import', {",
    "            method: 'POST',",
    "            headers: { 'Content-Type': 'application/json' },",
    "            body: JSON.stringify(config)",
    "          })",
    "          .then(function(response) { return response.json().then(function(data) { return { ok: response.ok, data: data }; }); })",
    "          .then(function(result) {",
    "            if (result.ok && result.data.success) {",
    "              showToast(result.data.message || 'Configuration imported.', 'success');",
    "              if (result.data.willRestart) {",
    "                if (result.data.deferred) {",
    "                  showPendingRestartDialog(result.data.activeStreams);",
    "                } else {",
    "                  waitForServerRestart();",
    "                }",
    "              }",
    "            } else {",
    "              throw new Error(result.data.message || result.data.error || 'Import failed');",
    "            }",
    "          })",
    "          .catch(function(err) { showToast('Failed to import configuration: ' + err.message, 'error'); });",
    "        }",
    "      } catch (err) {",
    "        showToast('Invalid JSON file: ' + err.message, 'error');",
    "      }",
    "      fileInput.value = '';",
    "    };",
    "    reader.readAsText(file);",
    "  };",

    // Export channels as JSON download.
    "  window.exportChannels = function() {",
    "    fetch('/config/channels/export')",
    "      .then(function(response) { return response.blob(); })",
    "      .then(function(blob) {",
    "        var url = window.URL.createObjectURL(blob);",
    "        var a = document.createElement('a');",
    "        a.href = url;",
    "        a.download = 'prismcast-channels.json';",
    "        document.body.appendChild(a);",
    "        a.click();",
    "        document.body.removeChild(a);",
    "        window.URL.revokeObjectURL(url);",
    "      })",
    "      .catch(function(err) { showToast('Failed to export channels: ' + err.message, 'error'); });",
    "  };",

    // Import channels from file.
    "  window.importChannels = function(fileInput) {",
    "    var file = fileInput.files[0];",
    "    if (!file) return;",
    "    var reader = new FileReader();",
    "    reader.onload = function(e) {",
    "      try {",
    "        var channels = JSON.parse(e.target.result);",
    "        if (confirm('Import these channels? This will replace all existing user channels.')) {",
    "          fetch('/config/channels/import', {",
    "            method: 'POST',",
    "            headers: { 'Content-Type': 'application/json' },",
    "            body: JSON.stringify(channels)",
    "          })",
    "          .then(function(response) {",
    "            if (response.ok) {",
    "              showToastAfterReload('Channels imported successfully.', 'success');",
    "            } else {",
    "              return response.text().then(function(text) { throw new Error(text); });",
    "            }",
    "          })",
    "          .catch(function(err) { showToast('Failed to import channels: ' + err.message, 'error'); });",
    "        }",
    "      } catch (err) {",
    "        showToast('Invalid JSON file: ' + err.message, 'error');",
    "      }",
    "      fileInput.value = '';",
    "    };",
    "    reader.readAsText(file);",
    "  };",

    // Import channels from M3U playlist file.
    "  window.importM3U = function(fileInput) {",
    "    var file = fileInput.files[0];",
    "    if (!file) return;",
    "    var replaceCheckbox = document.getElementById('m3u-replace-duplicates');",
    "    var conflictMode = (replaceCheckbox && replaceCheckbox.checked) ? 'replace' : 'skip';",
    "    var reader = new FileReader();",
    "    reader.onload = function(e) {",
    "      fetch('/config/channels/import-m3u', {",
    "        method: 'POST',",
    "        headers: { 'Content-Type': 'application/json' },",
    "        body: JSON.stringify({ content: e.target.result, conflictMode: conflictMode })",
    "      })",
    "      .then(function(response) { return response.json(); })",
    "      .then(function(data) {",
    "        if (data.success) {",
    "          var msg = 'M3U Import Complete\\n\\n';",
    "          msg += '\\u2713 ' + data.imported + ' channel(s) imported\\n';",
    "          if (data.replaced > 0) msg += '\\u21BB ' + data.replaced + ' channel(s) replaced\\n';",
    "          if (data.skipped > 0) msg += '\\u25CB ' + data.skipped + ' duplicate(s) skipped\\n';",
    "          if (data.errors && data.errors.length > 0) {",
    "            msg += '\\n! ' + data.errors.length + ' error(s):\\n';",
    "            for (var i = 0; i < Math.min(data.errors.length, 5); i++) {",
    "              msg += '  - ' + data.errors[i] + '\\n';",
    "            }",
    "            if (data.errors.length > 5) msg += '  ... and ' + (data.errors.length - 5) + ' more\\n';",
    "          }",
    "          if (data.imported > 0 || data.replaced > 0) { showToastAfterReload(msg, 'success'); }",
    "          else { showToast(msg, 'success'); }",
    "        } else {",
    "          showToast('M3U import failed: ' + (data.error || 'Unknown error'), 'error');",
    "        }",
    "      })",
    "      .catch(function(err) { showToast('Failed to import M3U: ' + err.message, 'error'); });",
    "      fileInput.value = '';",
    "    };",
    "    reader.readAsText(file);",
    "  };",

    // Client-side validation for numeric inputs.
    "  function validateInput(input) {",
    "    var min = input.min !== '' ? Number(input.min) : null;",
    "    var max = input.max !== '' ? Number(input.max) : null;",
    "    var value = Number(input.value);",
    "    var isValid = true;",
    "    if (input.type === 'number') {",
    "      if (isNaN(value)) { isValid = false; }",
    "      else if (min !== null && value < min) { isValid = false; }",
    "      else if (max !== null && value > max) { isValid = false; }",
    "    }",
    "    if (isValid) {",
    "      input.classList.remove('error');",
    "    } else {",
    "      input.classList.add('error');",
    "    }",
    "    return isValid;",
    "  }",

    // Subtab switching function.
    "  function switchSubtab(subtab, updateUrl) {",
    "    var btns = document.querySelectorAll('.subtab-btn');",
    "    var panels = document.querySelectorAll('.subtab-panel');",
    "    var settingsButtons = document.getElementById('settings-buttons');",
    "    for (var i = 0; i < btns.length; i++) {",
    "      btns[i].classList.remove('active');",
    "      btns[i].setAttribute('aria-selected', 'false');",
    "      if (btns[i].getAttribute('data-subtab') === subtab) {",
    "        btns[i].classList.add('active');",
    "        btns[i].setAttribute('aria-selected', 'true');",
    "      }",
    "    }",
    "    for (var j = 0; j < panels.length; j++) {",
    "      panels[j].classList.remove('active');",
    "      if (panels[j].id === 'subtab-' + subtab) {",
    "        panels[j].classList.add('active');",
    "      }",
    "    }",

    // Show/hide settings buttons based on subtab (hidden on backup subtab).
    "    if (settingsButtons) {",
    "      settingsButtons.style.display = (subtab === 'backup') ? 'none' : 'flex';",
    "    }",

    // Update localStorage and URL hash.
    "    try { localStorage.setItem('prismcast-config-subtab', subtab); } catch(e) {}",
    "    if (updateUrl !== false) {",
    "      var newHash = '#config/' + subtab;",
    "      if (window.location.hash !== newHash) {",
    "        window.location.hash = newHash;",
    "      }",
    "    }",
    "  }",

    // Expose for main tab script to use.
    "  window.switchConfigSubtab = switchSubtab;",

    // Attach click handlers to subtab buttons.
    "  var subtabBtns = document.querySelectorAll('.subtab-btn');",
    "  for (var i = 0; i < subtabBtns.length; i++) {",
    "    subtabBtns[i].addEventListener('click', function() {",
    "      switchSubtab(this.getAttribute('data-subtab'));",
    "    });",
    "  }",

    // Channel edit form show/hide functions.
    "  window.showEditForm = function(key) {",
    "    var displayRow = document.getElementById('display-row-' + key);",
    "    var editRow = document.getElementById('edit-row-' + key);",
    "    if (displayRow) displayRow.style.display = 'none';",
    "    if (editRow) {",
    "      editRow.style.display = '';",
    "      updateSelectorSuggestions('edit-url-' + key, 'edit-' + key + '-selectorList');",
    "      var urlInput = document.getElementById('edit-url-' + key);",
    "      if (urlInput && !urlInput.dataset.selectorBound) {",
    "        urlInput.addEventListener('input', function() { updateSelectorSuggestions('edit-url-' + key, 'edit-' + key + '-selectorList'); });",
    "        urlInput.dataset.selectorBound = 'true';",
    "      }",
    "    }",
    "  };",
    "  window.hideEditForm = function(key) {",
    "    var displayRow = document.getElementById('display-row-' + key);",
    "    var editRow = document.getElementById('edit-row-' + key);",
    "    if (displayRow) displayRow.style.display = '';",
    "    if (editRow) editRow.style.display = 'none';",
    "  };",

    // Hide add form and show the Add Channel button.
    "  window.hideAddForm = function() {",
    "    var addForm = document.getElementById('add-channel-form');",
    "    var addBtn = document.getElementById('add-channel-btn');",
    "    if (addForm) addForm.style.display = 'none';",
    "    if (addBtn) addBtn.style.display = 'inline-block';",
    "    if (addForm) addForm.querySelector('form').reset();",
    "  };",

    // Insert or replace channel rows in the table. Always removes existing rows with the same key first (handles both edits and overrides of builtin channels).
    "  window.insertChannelRow = function(html, key) {",
    "    var tbody = document.querySelector('.channel-table tbody');",
    "    if (!tbody || !html) return;",
    // Remove any existing rows with this key (edit or override of builtin).
    "    var oldDisplay = document.getElementById('display-row-' + key);",
    "    var oldEdit = document.getElementById('edit-row-' + key);",
    "    if (oldEdit) oldEdit.remove();",
    "    if (oldDisplay) oldDisplay.remove();",
    // Create temporary container to parse HTML.
    "    var temp = document.createElement('tbody');",
    "    temp.innerHTML = html.displayRow + (html.editRow || '');",
    "    var newDisplayRow = temp.firstElementChild;",
    "    var newEditRow = temp.children[1] || null;",
    // Find insertion point (alphabetical by key) and insert.
    "    var rows = tbody.querySelectorAll('tr[id^=\"display-row-\"]');",
    "    var inserted = false;",
    "    for (var i = 0; i < rows.length; i++) {",
    "      var rowKey = rows[i].id.replace('display-row-', '');",
    "      if (key < rowKey) {",
    "        tbody.insertBefore(newDisplayRow, rows[i]);",
    "        if (newEditRow) tbody.insertBefore(newEditRow, rows[i]);",
    "        inserted = true;",
    "        break;",
    "      }",
    "    }",
    "    if (!inserted) {",
    "      tbody.appendChild(newDisplayRow);",
    "      if (newEditRow) tbody.appendChild(newEditRow);",
    "    }",
    "    updateDisabledCount();",
    "  };",

    // Remove channel rows from the table.
    "  window.removeChannelRow = function(key) {",
    "    var displayRow = document.getElementById('display-row-' + key);",
    "    var editRow = document.getElementById('edit-row-' + key);",
    "    if (displayRow) displayRow.remove();",
    "    if (editRow) editRow.remove();",
    "    updateDisabledCount();",
    "  };",

    // Advanced fields toggle for channel forms.
    "  window.toggleAdvanced = function(prefix) {",
    "    var fields = document.getElementById(prefix + '-advanced');",
    "    var toggle = document.getElementById(prefix + '-toggle');",
    "    if (fields && toggle) {",
    "      if (fields.classList.contains('show')) {",
    "        fields.classList.remove('show');",
    "        toggle.textContent = '\\u25B6 Show Advanced Options';",
    "      } else {",
    "        fields.classList.add('show');",
    "        toggle.textContent = '\\u25BC Hide Advanced Options';",
    "      }",
    "    }",
    "  };",

    // Profile reference toggle.
    "  window.toggleProfileReference = function() {",
    "    var ref = document.getElementById('profile-reference');",
    "    if (ref) {",
    "      ref.style.display = ref.style.display === 'none' ? 'block' : 'none';",
    "    }",
    "  };",

    // Submit channel form via AJAX (add or edit).
    "  window.submitChannelForm = function(event, action) {",
    "    event.preventDefault();",
    "    var form = event.target;",
    "    var formData = new FormData(form);",
    "    var data = {};",
    "    for (var pair of formData.entries()) { data[pair[0]] = pair[1]; }",
    "    fetch('/config/channels', {",
    "      method: 'POST',",
    "      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },",
    "      body: JSON.stringify(data)",
    "    })",
    "    .then(function(response) { return response.json().then(function(d) { return { ok: response.ok, data: d }; }); })",
    "    .then(function(result) {",
    "      if (result.ok && result.data.success) {",
    "        showToast(result.data.message, 'success');",
    "        if (result.data.html) {",
    "          insertChannelRow(result.data.html, result.data.key);",
    "          if (action === 'add') {",
    "            hideAddForm();",
    "          } else {",
    "            hideEditForm(result.data.key);",
    "          }",
    "        } else {",
    "          showToast('Channel saved. Reload to see changes.', 'info');",
    "        }",
    "      } else if (result.data.errors) {",
    "        var errorMsgs = [];",
    "        for (var field in result.data.errors) { errorMsgs.push(field + ': ' + result.data.errors[field]); }",
    "        showToast('Validation errors: ' + errorMsgs.join(', '), 'error');",
    "      } else {",
    "        showToast(result.data.message || 'Failed to save channel.', 'error');",
    "      }",
    "    })",
    "    .catch(function(err) { showToast('Failed to save channel: ' + err.message, 'error'); });",
    "    return false;",
    "  };",

    // Delete channel via AJAX.
    "  window.deleteChannel = function(key) {",
    "    if (!confirm('Delete channel ' + key + '?')) return;",
    "    fetch('/config/channels', {",
    "      method: 'POST',",
    "      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },",
    "      body: JSON.stringify({ action: 'delete', key: key })",
    "    })",
    "    .then(function(response) { return response.json().then(function(d) { return { ok: response.ok, data: d }; }); })",
    "    .then(function(result) {",
    "      if (result.ok && result.data.success) {",
    "        showToast(result.data.message, 'success');",
    "        if (result.data.html) {",
    "          insertChannelRow(result.data.html, result.data.key || key);",
    "        } else {",
    "          removeChannelRow(result.data.key || key);",
    "        }",
    "      } else {",
    "        showToast(result.data.message || 'Failed to delete channel.', 'error');",
    "      }",
    "    })",
    "    .catch(function(err) { showToast('Failed to delete channel: ' + err.message, 'error'); });",
    "  };",

    // Toggle a single predefined channel's enabled/disabled state.
    "  window.togglePredefinedChannel = function(key, enable) {",
    "    fetch('/config/channels/toggle-predefined', {",
    "      method: 'POST',",
    "      headers: { 'Content-Type': 'application/json' },",
    "      body: JSON.stringify({ key: key, enabled: enable })",
    "    })",
    "    .then(function(response) { return response.json(); })",
    "    .then(function(result) {",
    "      if (result.success) {",
    "        showToast('Channel ' + key + ' ' + (enable ? 'enabled' : 'disabled') + '.', 'success');",
    "        updateChannelRowDisabledState(key, !enable);",
    "      } else {",
    "        showToast(result.error || 'Failed to toggle channel.', 'error');",
    "      }",
    "    })",
    "    .catch(function(err) { showToast('Failed to toggle channel: ' + err.message, 'error'); });",
    "  };",

    // Update a channel row's provider selection and profile cell in-place. Syncs the HTML selected attribute so filterChannelRows() restore logic works correctly.
    // We iterate _allOptions (if present) rather than querySelectorAll because filtered-out options are removed from the DOM but still tracked in the array.
    "  function updateChannelProviderUI(channelKey, variant, profile) {",
    "    var row = document.getElementById('display-row-' + channelKey);",
    "    if (!row) return;",
    "    var sel = row.querySelector('.provider-select');",
    "    if (sel) {",
    "      sel.value = variant;",
    "      var allOpts = sel._allOptions || Array.prototype.slice.call(sel.querySelectorAll('option'));",
    "      for (var oi = 0; oi < allOpts.length; oi++) {",
    "        if (allOpts[oi].value === variant) { allOpts[oi].setAttribute('selected', ''); }",
    "        else { allOpts[oi].removeAttribute('selected'); }",
    "      }",
    "    }",
    "    var profileCell = row.cells[3];",
    "    if (profileCell) {",
    "      if (profile) { profileCell.textContent = profile; }",
    "      else { profileCell.innerHTML = '<em>auto</em>'; }",
    "    }",
    "  }",

    // Update provider selection for a multi-provider channel.
    "  window.updateProviderSelection = function(selectElement) {",
    "    var channelKey = selectElement.getAttribute('data-channel');",
    "    var providerKey = selectElement.value;",
    "    fetch('/config/provider', {",
    "      method: 'POST',",
    "      headers: { 'Content-Type': 'application/json' },",
    "      body: JSON.stringify({ channel: channelKey, provider: providerKey })",
    "    })",
    "    .then(function(response) { return response.json(); })",
    "    .then(function(result) {",
    "      if (result.success) {",
    "        showToast('Provider updated. New streams will use the selected provider.', 'success');",
    "        updateChannelProviderUI(channelKey, providerKey, result.profile);",
    "      } else {",
    "        showToast(result.error || 'Failed to update provider.', 'error');",
    "      }",
    "    })",
    "    .catch(function(err) { showToast('Failed to update provider: ' + err.message, 'error'); });",
    "  };",

    // Toggle all predefined channels' enabled/disabled state.
    "  window.toggleAllPredefined = function(enable) {",
    "    fetch('/config/channels/toggle-all-predefined', {",
    "      method: 'POST',",
    "      headers: { 'Content-Type': 'application/json' },",
    "      body: JSON.stringify({ enabled: enable })",
    "    })",
    "    .then(function(response) { return response.json(); })",
    "    .then(function(result) {",
    "      if (result.success) {",
    "        showToast('All predefined channels ' + (enable ? 'enabled' : 'disabled') + '.', 'success');",
    "        var rows = document.querySelectorAll('tr[id^=\"display-row-\"]:not(.user-channel)');",
    "        for (var i = 0; i < rows.length; i++) {",
    "          var rowKey = rows[i].id.replace('display-row-', '');",
    "          setRowDisabledState(rowKey, !enable);",
    "        }",
    "        updateBulkToggleButton();",
    "        updateDisabledCount();",
    "      } else {",
    "        showToast(result.error || 'Failed to toggle channels.', 'error');",
    "      }",
    "    })",
    "    .catch(function(err) { showToast('Failed to toggle channels: ' + err.message, 'error'); });",
    "  };",

    // Set a channel row's disabled state without triggering count updates. Used by toggleAllPredefined for efficient bulk updates.
    "  function setRowDisabledState(key, disabled) {",
    "    var row = document.getElementById('display-row-' + key);",
    "    if (!row) return;",
    "    var btnGroup = row.querySelector('.btn-group');",
    "    if (!btnGroup) return;",
    "    if (disabled) {",
    "      row.classList.add('channel-disabled');",
    "      row.classList.remove('user-channel');",
    "      var loginBtn = btnGroup.querySelector('button[onclick*=\"startChannelLogin\"]');",
    "      if (loginBtn) loginBtn.remove();",
    "      var disableBtn = btnGroup.querySelector('.btn-disable');",
    "      if (disableBtn) {",
    "        disableBtn.className = 'btn btn-enable btn-sm';",
    "        disableBtn.textContent = 'Enable';",
    "        disableBtn.setAttribute('onclick', \"togglePredefinedChannel('\" + key + \"', true)\");",
    "      }",
    "    } else {",
    "      row.classList.remove('channel-disabled');",
    "      var enableBtn = btnGroup.querySelector('.btn-enable');",
    "      if (enableBtn) {",
    "        var newLoginBtn = document.createElement('button');",
    "        newLoginBtn.type = 'button';",
    "        newLoginBtn.className = 'btn btn-secondary btn-sm';",
    "        newLoginBtn.setAttribute('onclick', \"startChannelLogin('\" + key + \"')\");",
    "        newLoginBtn.textContent = 'Login';",
    "        btnGroup.insertBefore(newLoginBtn, enableBtn);",
    "        enableBtn.className = 'btn btn-disable btn-sm';",
    "        enableBtn.textContent = 'Disable';",
    "        enableBtn.setAttribute('onclick', \"togglePredefinedChannel('\" + key + \"', false)\");",
    "      }",
    "    }",
    "  }",

    // Update a single channel row's disabled state and refresh counts. Used by individual togglePredefinedChannel.
    "  function updateChannelRowDisabledState(key, disabled) {",
    "    setRowDisabledState(key, disabled);",
    "    updateBulkToggleButton();",
    "    updateDisabledCount();",
    "  };",

    // Update the bulk toggle button text based on current state.
    "  function updateBulkToggleButton() {",
    "    var btn = document.getElementById('bulk-toggle-btn');",
    "    if (!btn) return;",
    "    var disabledRows = document.querySelectorAll('tr.channel-disabled:not(.user-channel)');",
    "    var allRows = document.querySelectorAll('tr[id^=\"display-row-\"]:not(.user-channel)');",
    "    var allDisabled = disabledRows.length === allRows.length;",
    "    if (allDisabled) {",
    "      btn.textContent = 'Enable All Predefined';",
    "      btn.setAttribute('onclick', 'toggleAllPredefined(true)');",
    "    } else {",
    "      btn.textContent = 'Disable All Predefined';",
    "      btn.setAttribute('onclick', 'toggleAllPredefined(false)');",
    "    }",
    "  };",

    // Update the disabled channel count shown in the toolbar toggle label. Uses a union selector to avoid double-counting rows that are both disabled and
    // provider-unavailable.
    "  function updateDisabledCount() {",
    "    var countEl = document.getElementById('disabled-count');",
    "    if (!countEl) return;",
    "    var hiddenRows = document.querySelectorAll('tr.channel-disabled:not(.user-channel), tr.channel-unavailable');",
    "    countEl.textContent = String(hiddenRows.length);",
    "  };",

    // Provider filter: toggle a provider tag on/off.
    "  window.toggleProviderTag = function(checkbox) {",
    "    var menu = checkbox.closest('.provider-dropdown-menu');",
    "    if (!menu) return;",
    "    var checkboxes = menu.querySelectorAll('input[type=\"checkbox\"]:not(:disabled)');",
    "    var enabledTags = [];",
    "    for (var i = 0; i < checkboxes.length; i++) {",
    "      if (checkboxes[i].checked) enabledTags.push(checkboxes[i].getAttribute('data-tag'));",
    "    }",

    // If all checkboxes are checked, clear the filter (empty array = no filter).
    "    var allCheckboxes = menu.querySelectorAll('input[type=\"checkbox\"]');",
    "    var allChecked = true;",
    "    for (var j = 0; j < allCheckboxes.length; j++) {",
    "      if (!allCheckboxes[j].checked && !allCheckboxes[j].disabled) { allChecked = false; break; }",
    "    }",
    "    if (allChecked) enabledTags = [];",

    // POST to server.
    "    fetch('/config/provider-filter', {",
    "      method: 'POST',",
    "      headers: { 'Content-Type': 'application/json' },",
    "      body: JSON.stringify({ enabledProviders: enabledTags })",
    "    })",
    "    .then(function(r) { return r.json(); })",
    "    .then(function(result) {",
    "      if (result.success) {",
    "        updateProviderChips(enabledTags);",
    "        filterChannelRows(enabledTags);",
    "        updateBulkAssignOptions(enabledTags);",
    "        updateProviderFilterButton(enabledTags);",
    "        updateDisabledCount();",
    "      }",
    "    })",
    "    .catch(function(err) { showToast('Failed to update filter: ' + err.message, 'error'); });",
    "  };",

    // Remove a provider chip (uncheck the tag and update).
    "  window.removeProviderChip = function(tag) {",
    "    var menu = document.querySelector('.provider-dropdown-menu');",
    "    if (!menu) return;",
    "    var cb = menu.querySelector('input[data-tag=\"' + tag + '\"]');",
    "    if (cb) { cb.checked = false; toggleProviderTag(cb); }",
    "  };",

    // Update the provider filter button text.
    "  function updateProviderFilterButton(enabledTags) {",
    "    var btn = document.getElementById('provider-filter-btn');",
    "    if (!btn) return;",
    "    btn.innerHTML = (enabledTags.length > 0) ? 'Filtered &#9662;' : 'All Providers &#9662;';",
    "  };",

    // Rebuild the provider chips from the enabled tags.
    "  function updateProviderChips(enabledTags) {",
    "    var container = document.getElementById('provider-chips');",
    "    if (!container) return;",
    "    container.innerHTML = '';",
    "    if (enabledTags.length === 0) return;",
    "    var menu = document.querySelector('.provider-dropdown-menu');",
    "    for (var i = 0; i < enabledTags.length; i++) {",
    "      var tag = enabledTags[i];",
    "      if (tag === 'direct') continue;",
    "      var label = tag;",
    "      if (menu) {",
    "        var cb = menu.querySelector('input[data-tag=\"' + tag + '\"]');",
    "        if (cb && cb.parentElement) label = cb.parentElement.textContent.trim();",
    "      }",
    "      var chip = document.createElement('span');",
    "      chip.className = 'provider-chip';",
    "      chip.setAttribute('data-tag', tag);",
    "      chip.innerHTML = label + '<button type=\"button\" class=\"chip-close\" onclick=\"removeProviderChip(\\'' + tag + '\\')\">\\u00d7</button>';",
    "      container.appendChild(chip);",
    "    }",
    "  };",

    // Filter channel rows based on enabled provider tags. Toggles the channel-unavailable class on each row and updates Source column content. Filtered-out options
    // are removed from the DOM entirely rather than hidden — Safari ignores both the hidden attribute and display:none on option elements because they are rendered by
    // the OS native widget. All options (visible and removed) are stored in a _allOptions array on each select for reinsertion when the filter changes. Selection
    // restore priority: (1) the saved choice (HTML selected attribute, kept in sync by updateProviderSelection), (2) the previous visual selection, (3) first option.
    "  function filterChannelRows(enabledTags) {",
    "    var rows = document.querySelectorAll('tr[data-provider-tags]');",
    "    for (var i = 0; i < rows.length; i++) {",
    "      var tags = rows[i].getAttribute('data-provider-tags').split(',');",
    "      var available = true;",
    "      if (enabledTags.length > 0) {",
    "        available = false;",
    "        for (var j = 0; j < tags.length; j++) {",
    "          if (tags[j] === 'direct' || enabledTags.indexOf(tags[j]) !== -1) { available = true; break; }",
    "        }",
    "      }",
    "      if (available) { rows[i].classList.remove('channel-unavailable'); }",
    "      else { rows[i].classList.add('channel-unavailable'); }",

    // Update Source column elements: toggle between the no-provider label and the provider content (select or static name).
    "      var label = rows[i].querySelector('.no-provider-label');",
    "      var sel = rows[i].querySelector('.provider-select');",
    "      var name = rows[i].querySelector('.provider-name');",
    "      if (label) label.style.display = available ? 'none' : '';",
    "      if (name) name.style.display = available ? '' : 'none';",
    "      if (sel) {",
    "        sel.style.display = available ? '' : 'none';",

    // On first call, snapshot all options (including server-hidden ones) into a persistent array.
    "        if (!sel._allOptions) { sel._allOptions = Array.prototype.slice.call(sel.querySelectorAll('option')); }",
    "        var prevValue = sel.value;",
    "        sel.innerHTML = '';",
    "        var serverDefault = null;",
    "        var prevExists = false;",
    "        for (var k = 0; k < sel._allOptions.length; k++) {",
    "          var opt = sel._allOptions[k];",
    "          var oTag = opt.getAttribute('data-provider-tag');",
    "          var show = (enabledTags.length === 0) || oTag === 'direct' || enabledTags.indexOf(oTag) !== -1;",
    "          if (show) {",
    "            sel.appendChild(opt);",
    "            if (opt.hasAttribute('selected')) serverDefault = opt;",
    "            if (opt.value === prevValue) prevExists = true;",
    "          }",
    "        }",
    "        if (serverDefault) { sel.value = serverDefault.value; }",
    "        else if (prevExists) { sel.value = prevValue; }",
    "        else if (sel.options.length > 0) { sel.selectedIndex = 0; }",
    "      }",
    "    }",
    "  };",

    // Update bulk assign dropdown to only show enabled providers. Uses DOM removal like filterChannelRows because Safari ignores hidden/display:none on option
    // elements. The snapshot filters by truthy .value to exclude the "Choose provider..." placeholder (value="") so it is never removed from the DOM.
    "  function updateBulkAssignOptions(enabledTags) {",
    "    var select = document.getElementById('bulk-assign');",
    "    if (!select) return;",
    "    if (!select._allOptions) {",
    "      select._allOptions = [];",
    "      var all = select.querySelectorAll('option');",
    "      for (var a = 0; a < all.length; a++) { if (all[a].value) select._allOptions.push(all[a]); }",
    "    }",
    "    for (var i = 0; i < select._allOptions.length; i++) {",
    "      var opt = select._allOptions[i];",
    "      if (opt.parentNode === select) { select.removeChild(opt); }",
    "    }",
    "    for (var j = 0; j < select._allOptions.length; j++) {",
    "      var opt2 = select._allOptions[j];",
    "      if (enabledTags.length === 0 || opt2.value === 'direct' || enabledTags.indexOf(opt2.value) !== -1) {",
    "        select.appendChild(opt2);",
    "      }",
    "    }",
    "    select.value = '';",
    "  };",

    // Bulk assign all channels to a specific provider. Updates all dropdowns and profile cells in-place.
    "  window.bulkAssignProvider = function(selectEl) {",
    "    var providerTag = selectEl.value;",
    "    if (!providerTag) return;",
    "    selectEl.value = '';",
    "    fetch('/config/provider-bulk-assign', {",
    "      method: 'POST',",
    "      headers: { 'Content-Type': 'application/json' },",
    "      body: JSON.stringify({ provider: providerTag })",
    "    })",
    "    .then(function(r) { return r.json(); })",
    "    .then(function(result) {",
    "      if (result.success) {",
    "        var msg = result.affected + ' of ' + result.total + ' channel(s) updated.';",
    "        var undoAction = null;",
    "        if (result.affected > 0 && result.previousSelections) {",
    "          var prevSelections = result.previousSelections;",
    "          undoAction = { label: 'Undo', onclick: function() { restoreBulkProviders(prevSelections); } };",
    "        }",
    "        showToast(msg, 'success', undoAction ? 10000 : undefined, undoAction);",
    "        if (result.selections) {",
    "          for (var key in result.selections) {",
    "            var sel = result.selections[key];",
    "            updateChannelProviderUI(key, sel.variant, sel.profile);",
    "          }",
    "        }",
    "      } else {",
    "        showToast(result.error || 'Failed to assign.', 'error');",
    "      }",
    "    })",
    "    .catch(function(err) { showToast('Failed to assign: ' + err.message, 'error'); });",
    "  };",

    // Restore previous provider selections (undo bulk assign). Sends the previousSelections map to the server and updates the UI with the restored selections.
    "  function restoreBulkProviders(prevSelections) {",
    "    fetch('/config/provider-bulk-restore', {",
    "      method: 'POST',",
    "      headers: { 'Content-Type': 'application/json' },",
    "      body: JSON.stringify({ selections: prevSelections })",
    "    })",
    "    .then(function(r) { return r.json(); })",
    "    .then(function(result) {",
    "      if (result.success) {",
    "        showToast('Bulk assign reverted.', 'success');",
    "        if (result.selections) {",
    "          for (var key in result.selections) {",
    "            var sel = result.selections[key];",
    "            updateChannelProviderUI(key, sel.variant, sel.profile);",
    "          }",
    "        }",
    "      } else {",
    "        showToast(result.error || 'Failed to revert.', 'error');",
    "      }",
    "    })",
    "    .catch(function(err) { showToast('Failed to revert: ' + err.message, 'error'); });",
    "  }",

    // Close all open dropdown menus.
    "  function closeDropdowns() {",
    "    var menus = document.querySelectorAll('.dropdown-menu.show');",
    "    for (var i = 0; i < menus.length; i++) menus[i].classList.remove('show');",
    "  };",
    "  window.closeDropdowns = closeDropdowns;",

    // Toggle a dropdown menu open or closed. Closes any other open dropdowns first.
    "  window.toggleDropdown = function(btn) {",
    "    var menu = btn.nextElementSibling;",
    "    if (!menu) return;",
    "    var isOpen = menu.classList.contains('show');",
    "    closeDropdowns();",
    "    if (!isOpen) menu.classList.add('show');",
    "  };",

    // Close dropdowns when clicking outside.
    "  document.addEventListener('click', function(e) {",
    "    if (!e.target.closest('.dropdown')) closeDropdowns();",
    "  });",

    // Toggle visibility of disabled predefined channels and persist preference.
    "  window.toggleDisabledVisibility = function() {",
    "    var table = document.querySelector('.channel-table');",
    "    var checkbox = document.getElementById('show-disabled-toggle');",
    "    if (!table || !checkbox) return;",
    "    if (checkbox.checked) {",
    "      table.classList.remove('hide-disabled');",
    "      localStorage.setItem('prismcast-show-disabled-channels', 'true');",
    "    } else {",
    "      table.classList.add('hide-disabled');",
    "      localStorage.removeItem('prismcast-show-disabled-channels');",
    "    }",
    "  };",

    // Populate channel selector datalist based on the URL field value. Looks up known selectors from predefined channels that share the same domain.
    "  function updateSelectorSuggestions(urlInputId, datalistId) {",
    "    var urlInput = document.getElementById(urlInputId);",
    "    var datalist = document.getElementById(datalistId);",
    "    if (!urlInput || !datalist) return;",
    "    datalist.innerHTML = '';",
    "    try {",
    "      var hostname = new URL(urlInput.value).hostname;",
    "      var entries = (typeof channelSelectorsByDomain !== 'undefined') ? channelSelectorsByDomain[hostname] : null;",
    "      if (entries) {",
    "        for (var i = 0; i < entries.length; i++) {",
    "          var opt = document.createElement('option');",
    "          opt.value = entries[i].value;",
    "          opt.label = entries[i].label;",
    "          datalist.appendChild(opt);",
    "        }",
    "      }",
    "    } catch (e) {}",
    "  };",

    // Initialize disabled channel toggle, provider filter, URL input listeners, and pending toast on page load.
    "  (function() {",

    // Show any toast queued by showToastAfterReload() before the last page reload.
    "    var pending = sessionStorage.getItem('pendingToast');",
    "    if (pending) {",
    "      sessionStorage.removeItem('pendingToast');",
    "      try { var pt = JSON.parse(pending); showToast(pt.message, pt.type); } catch (e) {}",
    "    }",
    "    if (localStorage.getItem('prismcast-show-disabled-channels') === 'true') {",
    "      var table = document.querySelector('.channel-table');",
    "      var checkbox = document.getElementById('show-disabled-toggle');",
    "      if (table) table.classList.remove('hide-disabled');",
    "      if (checkbox) checkbox.checked = true;",
    "    }",

    // Run filterChannelRows on page load when a provider filter is active. The server renders filtered options with the hidden attribute, but Safari ignores it on
    // option elements. This initial pass removes those options from the DOM to enforce the filter.
    "    var menu = document.querySelector('.provider-dropdown-menu');",
    "    if (menu) {",
    "      var cbs = menu.querySelectorAll('input[type=\"checkbox\"]:not(:disabled)');",
    "      var tags = [];",
    "      var allChecked = true;",
    "      for (var ci = 0; ci < cbs.length; ci++) {",
    "        if (cbs[ci].checked) { tags.push(cbs[ci].getAttribute('data-tag')); }",
    "        else { allChecked = false; }",
    "      }",
    "      if (!allChecked) { filterChannelRows(tags); updateBulkAssignOptions(tags); }",
    "    }",
    "    var addUrlInput = document.getElementById('add-url');",
    "    if (addUrlInput) {",
    "      addUrlInput.addEventListener('input', function() { updateSelectorSuggestions('add-url', 'add-selectorList'); });",
    "      updateSelectorSuggestions('add-url', 'add-selectorList');",
    "    }",
    "  })();",

    // Login modal state tracking.
    "  var loginStatusInterval = null;",

    // Start login mode for a channel. Opens browser window and shows modal.
    "  window.startChannelLogin = function(channel) {",
    "    fetch('/auth/login', {",
    "      method: 'POST',",
    "      headers: { 'Content-Type': 'application/json' },",
    "      body: JSON.stringify({ channel: channel })",
    "    })",
    "    .then(function(response) { return response.json(); })",
    "    .then(function(result) {",
    "      if (result.success) {",
    "        showToast('Browser window opened. Complete authentication.', 'info');",
    "        showLoginModal();",
    "        startLoginStatusPolling();",
    "      } else {",
    "        showToast(result.error || 'Failed to start login.', 'error');",
    "      }",
    "    })",
    "    .catch(function(err) { showToast('Failed to start login: ' + err.message, 'error'); });",
    "  };",

    // End login mode. Closes browser tab and hides modal.
    "  window.endLogin = function() {",
    "    stopLoginStatusPolling();",
    "    fetch('/auth/done', { method: 'POST' })",
    "    .then(function() {",
    "      hideLoginModal();",
    "      showToast('Authentication complete.', 'success');",
    "    })",
    "    .catch(function(err) { showToast('Error ending login: ' + err.message, 'error'); hideLoginModal(); });",
    "  };",

    // Show the login modal.
    "  function showLoginModal() {",
    "    var modal = document.getElementById('login-modal');",
    "    if (modal) modal.style.display = 'flex';",
    "  }",

    // Hide the login modal.
    "  function hideLoginModal() {",
    "    var modal = document.getElementById('login-modal');",
    "    if (modal) modal.style.display = 'none';",
    "  }",

    // Start polling login status to detect when tab is closed externally.
    "  function startLoginStatusPolling() {",
    "    stopLoginStatusPolling();",
    "    loginStatusInterval = setInterval(function() {",
    "      fetch('/auth/status')",
    "      .then(function(response) { return response.json(); })",
    "      .then(function(status) {",
    "        if (!status.active) {",
    "          stopLoginStatusPolling();",
    "          hideLoginModal();",
    "          showToast('Login session ended.', 'info');",
    "        }",
    "      })",
    "      .catch(function() { });",
    "    }, 1000);",
    "  }",

    // Stop polling login status.
    "  function stopLoginStatusPolling() {",
    "    if (loginStatusInterval) {",
    "      clearInterval(loginStatusInterval);",
    "      loginStatusInterval = null;",
    "    }",
    "  }",

    // Attach validation and modified indicator handlers to form inputs.
    "  var form = document.getElementById('settings-form');",
    "  if (form) {",
    "    var inputs = form.querySelectorAll('input, select');",
    "    for (var k = 0; k < inputs.length; k++) {",
    "      var eventType = inputs[k].type === 'checkbox' ? 'change' : 'input';",
    "      inputs[k].addEventListener(eventType, function() {",
    "        validateInput(this);",
    "        updateModifiedIndicator(this);",
    "      });",
    "    }",

    // Wire up all checkboxes that have dependent fields. The updateDependentFields function is defined at the top level alongside the other helpers.
    "    var checkboxes = form.querySelectorAll('input[type=\"checkbox\"]');",
    "    for (var c = 0; c < checkboxes.length; c++) {",
    "      (function(cb) {",
    "        cb.addEventListener('change', function() { updateDependentFields(cb.id); });",
    "      })(checkboxes[c]);",
    "    }",

    // Attach preset change handler to auto-fill bitrate and frame rate.
    "    var presetSelect = document.getElementById('streaming-qualityPreset');",
    "    if (presetSelect) {",
    "      presetSelect.addEventListener('change', function() { onPresetChange(this.value); });",
    "    }",
    "  }",

    // Initialize subtab on load: hash > localStorage > default.
    "  var initialSubtab = window.initialHashSubtab;",
    "  if (!initialSubtab) {",
    "    try { initialSubtab = localStorage.getItem('prismcast-config-subtab'); } catch(e) {}",
    "  }",
    "  if (initialSubtab && document.querySelector('.subtab-btn[data-subtab=\"' + initialSubtab + '\"]')) {",
    "    switchSubtab(initialSubtab, false);",
    "  }",

    "})();",
    "</script>"
  ].join("\n");
}

/**
 * Generates additional CSS styles specific to the landing page. Uses CSS custom properties for theme support.
 * @returns CSS styles as a string.
 */
function generateLandingPageStyles(): string {

  return [

    // Override header to use space-between for logo/title on left and status on right.
    ".header { justify-content: space-between; }",
    ".header-left { display: flex; align-items: center; gap: 20px; }",

    // Header links (GitHub, More by HJD).
    ".header-links { display: flex; align-items: center; gap: 8px; font-size: 13px; }",
    ".header-links a { color: var(--text-muted); text-decoration: none; transition: color 0.2s; }",
    ".header-links a:hover { color: var(--text-primary); }",
    ".header-links-sep { color: var(--text-muted); }",

    // Header status bar styles.
    ".header-status { display: flex; gap: 20px; align-items: center; font-size: 13px; color: var(--text-secondary); }",
    ".header-status span { white-space: nowrap; }",

    // Stream count popover. Clickable when streams are active; popover drops from the right edge of the header.
    "#stream-count.clickable { cursor: pointer; }",
    "#stream-count.clickable:hover { color: var(--text-primary); }",
    ".stream-popover .dropdown-menu { right: 0; left: auto; min-width: 220px; }",
    ".stream-popover-row { display: flex; align-items: center; gap: 8px; padding: 6px 12px; font-size: 13px; white-space: nowrap; }",
    ".stream-popover-logo { height: 18px; width: auto; max-width: 80px; vertical-align: middle; }",
    ".stream-popover-channel { color: var(--text-primary); }",
    ".stream-popover-duration { color: var(--text-muted); margin-left: auto; }",

    // Subtab styles for Configuration tab.
    ".subtab-bar { display: flex; border-bottom: 1px solid var(--border-default); margin-bottom: 20px; gap: 2px; flex-wrap: wrap; }",
    ".subtab-btn { padding: 8px 16px; border: none; background: var(--subtab-bg); cursor: pointer; font-size: 13px; font-weight: 500; ",
    "color: var(--tab-text); border-radius: var(--radius-md) var(--radius-md) 0 0; transition: all 0.2s; }",
    ".subtab-btn:hover { background: var(--subtab-bg-hover); color: var(--tab-text-hover); }",
    ".subtab-btn.active { background: var(--subtab-bg-active); color: var(--subtab-text-active); border-bottom: 2px solid var(--subtab-border-active); }",
    ".subtab-panel { display: none; }",
    ".subtab-panel.active { display: block; }",

    // Panel header layout for description and reset link alignment.
    ".panel-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }",

    // Settings panel description styling (replaces redundant header titles).
    ".settings-panel-description { margin: 0; font-size: 15px; color: var(--text-primary); }",
    ".settings-panel-description p { margin: 0; }",
    ".description-hint { font-size: 13px; color: var(--text-secondary); margin-top: 4px; }",

    // Streams table container - outer border with rounded corners.
    "#streams-container { border: 1px solid var(--border-default); border-radius: var(--radius-md); overflow: hidden; margin-bottom: 20px; }",

    // Streams table - minimal design with no borders between columns.
    ".streams-table { width: 100%; border-collapse: collapse; margin: 0; }",
    ".streams-table td { padding: 6px 10px; border: none; color: var(--text-primary); vertical-align: middle; }",
    ".streams-table td:first-child { padding-left: 12px; }",
    ".streams-table td:last-child { padding-right: 12px; }",
    ".streams-table .empty-row td { padding: 10px 12px; text-align: center; color: var(--text-muted); }",
    ".streams-table .empty-row:hover { background: transparent; }",
    ".streams-table .stream-row { cursor: pointer; }",
    ".streams-table .stream-row:hover { background: var(--table-row-hover); }",
    ".streams-table .chevron { width: 20px; color: var(--text-muted); font-size: 10px; }",
    ".streams-table .stream-info { width: 180px; max-width: 180px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: 500; font-size: 13px; }",
    ".streams-table .stream-duration { font-weight: 400; color: var(--text-secondary); }",
    ".streams-table .channel-logo { height: 24px; width: auto; max-width: 100px; vertical-align: middle; margin-right: 4px; }",
    ".streams-table .channel-text { vertical-align: middle; }",
    ".streams-table .stream-show { max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--text-secondary); font-size: 13px; }",
    ".streams-table .stream-health { text-align: right; white-space: nowrap; font-size: 13px; }",
    ".streams-table .stream-details td { padding: 10px 12px 12px 32px; background: var(--surface-sunken); }",
    ".streams-table .details-content { font-size: 12px; color: var(--text-secondary); }",
    ".streams-table .details-header { display: flex; justify-content: space-between; align-items: flex-start; gap: 20px; margin-bottom: 10px; }",
    ".streams-table .details-url { word-break: break-all; flex: 1; min-width: 0; }",
    ".streams-table .details-started { white-space: nowrap; flex-shrink: 0; }",
    ".streams-table .details-metrics { display: flex; align-items: baseline; gap: 20px; }",
    ".streams-table .details-issue { flex: 1; min-width: 0; }",
    ".streams-table .details-recovery { white-space: nowrap; flex-shrink: 0; }",
    ".streams-table .details-memory { white-space: nowrap; flex-shrink: 0; }",
    ".streams-table .client-count { font-size: 0.85em; color: var(--text-muted); margin-right: 8px; white-space: nowrap; }",

    // Log viewer styles.
    ".log-viewer { background: var(--dark-surface-bg); color: var(--dark-text-secondary); padding: 15px; border-radius: var(--radius-lg); ",
    "font-family: 'SF Mono', Monaco, monospace; font-size: 12px; height: 500px; overflow-y: auto; white-space: pre-wrap; word-break: break-all; }",
    ".log-viewer::-webkit-scrollbar { width: 8px; }",
    ".log-viewer::-webkit-scrollbar-track { background: var(--dark-scrollbar-track); }",
    ".log-viewer::-webkit-scrollbar-thumb { background: var(--dark-scrollbar-thumb); border-radius: var(--radius-md); }",
    ".log-viewer::-webkit-scrollbar-thumb:hover { background: var(--dark-scrollbar-thumb-hover); }",
    ".log-entry { color: var(--dark-text-secondary); }",
    ".log-error { color: var(--dark-text-error); }",
    ".log-warn { color: var(--dark-text-warn); }",
    ".log-debug { color: var(--dark-text-debug); }",
    ".log-muted { color: var(--dark-text-muted); }",
    ".log-connecting { color: var(--dark-text-muted); }",

    // Channel table styles. The wrapper enables horizontal scrolling on small screens.
    ".channel-table-wrapper { overflow-x: auto; margin-bottom: 20px; }",
    ".channel-table { width: 100%; border-collapse: collapse; table-layout: fixed; min-width: 650px; }",
    ".channel-table th, .channel-table td { padding: 8px 10px; text-align: left; border-bottom: 1px solid var(--border-default); ",
    "overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }",
    ".channel-table th { background: var(--table-header-bg); font-weight: 600; font-size: 13px; }",
    ".channel-table tr:hover { background: var(--table-row-hover); }",
    ".channel-table .col-key { width: 170px; }",
    ".channel-table .col-name { width: 250px; }",
    ".channel-table .col-source { width: 150px; }",
    ".channel-table .col-profile { width: 140px; }",
    ".channel-table .col-actions { width: 170px; white-space: nowrap; overflow: visible; }",
    ".provider-select { width: 100%; padding: 2px 4px; font-size: 12px; border: 1px solid var(--form-input-border); ",
    "border-radius: 3px; background: var(--form-input-bg); color: var(--text-primary); }",

    // Responsive: hide Profile on tablets, hide Key and Profile on phones.
    "@media (max-width: 1024px) { .channel-table .col-profile, .channel-table td:nth-child(4), .channel-table th:nth-child(4) { display: none; } }",
    "@media (max-width: 768px) { .channel-table .col-key, .channel-table td:nth-child(1), .channel-table th:nth-child(1) { display: none; } }",

    // User channel row tinting to distinguish custom/override channels from predefined.
    ".channel-table tr.user-channel { background: var(--user-channel-tint); }",
    ".channel-table tr.user-channel:hover { background: var(--user-channel-tint-hover); }",

    // Disabled predefined channel row styling and hide-disabled toggle.
    ".channel-table tr.channel-disabled { opacity: 0.5; }",
    ".channel-table tr.channel-disabled td { color: var(--text-tertiary); }",
    ".channel-table tr.channel-disabled code { color: var(--text-tertiary); }",
    ".channel-table.hide-disabled tr.channel-disabled { display: none; }",

    // Provider-filtered channel row styling. Uses reduced opacity and italic text to distinguish from manually disabled rows. The compound selector ensures that rows
    // which are both disabled and provider-filtered render at the disabled-level opacity (0.5) rather than the more aggressive unavailable-level opacity (0.4).
    ".channel-table tr.channel-unavailable { opacity: 0.4; font-style: italic; }",
    ".channel-table tr.channel-unavailable td { color: var(--text-tertiary); }",
    ".channel-table tr.channel-unavailable.channel-disabled { opacity: 0.5; }",
    ".channel-table.hide-disabled tr.channel-unavailable { display: none; }",
    ".no-provider-label { color: var(--text-tertiary); font-size: 12px; }",

    // Provider filter toolbar layout.
    ".provider-toolbar { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-bottom: 10px; }",
    ".provider-toolbar .toolbar-group { display: flex; align-items: center; gap: 6px; }",
    ".provider-toolbar .toolbar-label { font-size: 13px; color: var(--text-secondary); white-space: nowrap; }",
    ".provider-toolbar .toolbar-spacer { flex: 1; }",

    // Provider dropdown multi-select.
    ".provider-dropdown-menu { min-width: 200px; max-height: 300px; overflow-y: auto; }",
    ".provider-option { display: flex; align-items: center; gap: 6px; padding: 5px 12px; font-size: 13px; cursor: pointer; color: var(--text-primary); }",
    ".provider-option:hover { background: var(--surface-sunken); }",
    ".provider-option input[type=\"checkbox\"] { margin: 0; }",

    // Provider chips.
    ".provider-chips { display: flex; flex-wrap: wrap; align-items: center; gap: 4px; }",
    ".provider-chip { display: inline-flex; align-items: center; gap: 4px; background: var(--surface-elevated); border: 1px solid var(--border-default); ",
    "border-radius: 12px; padding: 2px 8px 2px 10px; font-size: 12px; color: var(--text-secondary); min-height: 24px; }",
    ".chip-close { background: none; border: none; cursor: pointer; font-size: 14px; line-height: 1; padding: 0 2px; color: var(--text-muted); ",
    "transition: color 0.2s; }",
    ".chip-close:hover { color: var(--text-primary); }",

    // Bulk assign dropdown.
    ".bulk-assign-select { font-size: 13px; padding: 4px 8px; border: 1px solid var(--border-default); border-radius: var(--radius-md); ",
    "background: var(--surface-default); color: var(--text-primary); cursor: pointer; }",

    // Responsive: stack provider toolbar groups vertically on small screens.
    "@media (max-width: 768px) { .provider-toolbar { flex-direction: column; align-items: flex-start; } }",

    // Enable/Disable button styling.
    ".btn-enable { background: var(--status-success-bg); color: var(--status-success-text); border: 1px solid var(--status-success-border); }",
    ".btn-enable:hover { background: var(--status-success-border); }",
    ".btn-disable { background: var(--surface-elevated); color: var(--text-secondary); border: 1px solid var(--border-default); }",
    ".btn-disable:hover { border-color: var(--text-secondary); }",

    // Channel toolbar with operation buttons and display controls.
    ".channel-toolbar { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-top: 10px; margin-bottom: 15px; }",
    ".channel-toolbar .toolbar-group { display: flex; align-items: center; gap: 6px; }",
    ".channel-toolbar .toolbar-spacer { flex: 1; }",
    ".channel-toolbar .toggle-label { font-size: 12px; color: var(--text-secondary); cursor: pointer; display: flex; align-items: center; gap: 4px; ",
    "user-select: none; }",

    // Dropdown menu used by the Import button in the channel toolbar.
    ".dropdown { position: relative; display: inline-block; }",
    ".dropdown-menu { display: none; position: absolute; top: 100%; left: 0; z-index: 1000; min-width: 180px; padding: 4px 0; margin-top: 2px; ",
    "background: var(--surface-overlay); border: 1px solid var(--border-default); border-radius: var(--radius-md); ",
    "box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15); }",
    ".dropdown-menu.show { display: block; }",
    ".dropdown-item { padding: 6px 12px; font-size: 13px; cursor: pointer; color: var(--text-primary); }",
    ".dropdown-item:hover { background: var(--surface-sunken); }",
    ".dropdown-option { display: block; padding: 2px 12px 6px 24px; font-size: 12px; color: var(--text-secondary); cursor: pointer; user-select: none; }",
    ".dropdown-divider { height: 1px; margin: 4px 0; background: var(--border-default); }",

    // Channel form styles. Inputs use full width; selects use width classes from ui.ts for consistency with settings forms.
    ".channel-form { background: var(--form-bg); border: 1px solid var(--border-default); border-radius: var(--radius-lg); padding: 20px; margin-bottom: 20px; }",
    ".channel-form h3 { margin-top: 0; margin-bottom: 15px; color: var(--text-heading-secondary); }",
    ".channel-form .form-row { margin-bottom: 4px; }",
    ".channel-form .form-row:last-child { margin-bottom: 0; }",
    ".channel-form .form-input { width: 100%; box-sizing: border-box; }",

    // Advanced toggle styles.
    ".advanced-toggle { color: var(--interactive-primary); cursor: pointer; font-size: 13px; margin-top: 5px; margin-bottom: 15px; }",
    ".advanced-toggle:hover { text-decoration: underline; }",
    ".advanced-fields { display: none; }",
    ".advanced-fields.show { display: block; }",

    // Profile reference section styles.
    ".profile-reference { background: var(--surface-elevated); border: 1px solid var(--border-default); border-radius: var(--radius-lg); margin: 20px 0; ",
    "padding: 20px; }",
    ".profile-reference-header { display: flex; justify-content: space-between; align-items: flex-start; }",
    ".profile-reference h3 { margin: 0 0 10px 0; color: var(--text-heading-secondary); }",
    ".profile-reference-close { color: var(--text-secondary); font-size: 18px; text-decoration: none; padding: 0 5px; }",
    ".profile-reference-close:hover { color: var(--text-primary); }",
    ".reference-intro { color: var(--text-secondary); font-size: 13px; margin-bottom: 20px; }",
    ".profile-category { margin-bottom: 20px; }",
    ".profile-category:last-child { margin-bottom: 0; }",
    ".profile-category h4 { color: var(--text-heading-secondary); font-size: 14px; font-weight: 600; margin: 0 0 8px 0; }",
    ".category-desc { color: var(--text-tertiary); font-size: 12px; margin: 0 0 10px 0; }",
    ".profile-list { margin: 0; padding: 0; }",
    ".profile-list dt { font-family: var(--font-mono); font-size: 13px; font-weight: 600; margin-top: 10px; color: var(--text-primary); }",
    ".profile-list dt:first-child { margin-top: 0; }",
    ".profile-list dd { color: var(--text-secondary); font-size: 13px; margin: 4px 0 0 0; }",
    ".selector-guide-heading { margin-top: 20px !important; border-top: 1px solid var(--border-default); padding-top: 16px; }",

    // Other landing page styles.
    ".endpoint code { font-size: 13px; }",

    // Modified value indicator styling.
    ".form-group.modified { border-left: 3px solid var(--interactive-primary); padding-left: 12px; }",
    ".modified-dot { display: inline-block; width: 8px; height: 8px; background: var(--interactive-primary); border-radius: 50%; margin-right: 6px; ",
    "vertical-align: middle; }",

    // Per-setting reset button styling.
    ".btn-reset { background: transparent; border: 1px solid var(--border-default); border-radius: var(--radius-md); padding: 4px 8px; margin-left: 8px; ",
    "cursor: pointer; font-size: 14px; color: var(--text-secondary); transition: all 0.15s ease; }",
    ".btn-reset:hover { background: var(--surface-elevated); border-color: var(--interactive-primary); color: var(--interactive-primary); }",

    // Backup subtab section styling.
    ".backup-group { margin-bottom: 35px; }",
    ".backup-group-title { font-size: 16px; font-weight: 600; margin-bottom: 15px; color: var(--text-heading); ",
    "padding-bottom: 8px; border-bottom: 1px solid var(--border-default); }",
    ".backup-section { margin-bottom: 20px; padding: 20px; background: var(--surface-elevated); border-radius: var(--radius-lg); ",
    "border: 1px solid var(--border-default); }",
    ".backup-section h3 { margin-top: 0; margin-bottom: 10px; color: var(--text-heading-secondary); font-size: 15px; }",
    ".backup-section p { color: var(--text-secondary); margin-bottom: 15px; font-size: 14px; }",
    ".backup-section code { background: var(--surface-code); padding: 2px 5px; border-radius: 3px; font-size: 12px; }",
    "#import-settings-file, #import-channels-file, #import-m3u-file { display: none; }",
    ".btn-export { background: var(--surface-elevated); border: 1px solid var(--border-default); color: var(--text-primary); ",
    "padding: 10px 20px; border-radius: var(--radius-md); font-size: 14px; cursor: pointer; transition: all 0.15s ease; }",
    ".btn-export:hover { border-color: var(--interactive-primary); color: var(--interactive-primary); }",
    ".btn-import { background: var(--surface-elevated); border: 1px solid var(--border-default); color: var(--text-primary); ",
    "padding: 10px 20px; border-radius: var(--radius-md); font-size: 14px; cursor: pointer; transition: all 0.15s ease; }",
    ".btn-import:hover { border-color: var(--interactive-primary); color: var(--interactive-primary); }",

    // Login modal styles for channel authentication.
    ".login-modal { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0, 0, 0, 0.5); display: flex; ",
    "align-items: center; justify-content: center; z-index: 1000; }",
    ".login-modal-content { background: var(--surface-overlay); padding: 30px; border-radius: var(--radius-lg); max-width: 450px; width: 90%; ",
    "box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3); }",
    ".login-modal-content h3 { margin-top: 0; margin-bottom: 15px; color: var(--text-heading); }",
    ".login-modal-content p { color: var(--text-secondary); margin-bottom: 15px; font-size: 14px; line-height: 1.5; }",
    ".login-modal-hint { font-size: 13px; color: var(--text-muted); }",
    ".login-modal-buttons { margin-top: 20px; text-align: right; }",

    // Restart dialog modal styles for pending restart notification.
    ".restart-modal { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0, 0, 0, 0.5); display: none; ",
    "align-items: center; justify-content: center; z-index: 1000; }",
    ".restart-modal-content { background: var(--surface-overlay); padding: 30px; border-radius: var(--radius-lg); max-width: 400px; width: 90%; ",
    "box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3); text-align: center; }",
    ".restart-modal-content h3 { margin-top: 0; margin-bottom: 15px; color: var(--text-heading); }",
    ".restart-modal-content p { color: var(--text-secondary); margin-bottom: 0; font-size: 14px; line-height: 1.5; }",
    ".restart-modal-status { margin: 16px 0; color: var(--text-muted); font-size: 13px; }",
    ".restart-modal-buttons { display: flex; gap: 12px; justify-content: center; margin-top: 20px; }",
    ".btn-danger { background: var(--interactive-danger); color: white; border: none; padding: 10px 20px; border-radius: var(--radius-md); ",
    "font-size: 14px; cursor: pointer; transition: all 0.15s ease; }",
    ".btn-danger:hover { opacity: 0.9; }",

    // Toast notification container: fixed top-right, above all modals.
    ".toast-container { position: fixed; top: 20px; right: 20px; z-index: 1001; display: flex; flex-direction: column; gap: 8px; pointer-events: none; }",

    // Individual toast: themed colors, slide-in animation, close button.
    ".toast { padding: 12px 36px 12px 16px; border-radius: var(--radius-md); box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15); min-width: 280px; max-width: 420px; ",
    "font-size: 13px; line-height: 1.4; white-space: pre-line; position: relative; pointer-events: auto; animation: toastIn 0.3s ease-out; }",
    ".toast.toast-exit { animation: toastOut 0.3s ease-in forwards; }",

    // Type variants using existing theme status variables.
    ".toast.success { background: var(--status-success-bg); border: 1px solid var(--status-success-border); color: var(--status-success-text); }",
    ".toast.error { background: var(--status-error-bg); border: 1px solid var(--status-error-border); color: var(--status-error-text); }",
    ".toast.warning { background: var(--status-warning-bg); border: 1px solid var(--status-warning-border); color: var(--status-warning-text); }",
    ".toast.info { background: var(--status-info-bg); border: 1px solid var(--status-info-border); color: var(--status-info-text); }",

    // Close button positioned top-right within each toast.
    ".toast-close { position: absolute; top: 8px; right: 8px; background: none; border: none; cursor: pointer; font-size: 16px; line-height: 1; padding: 0 4px; ",
    "color: inherit; opacity: 0.6; }",
    ".toast-close:hover { opacity: 1; }",

    // Action button for toasts with an undo or similar inline action.
    ".toast-action { display: inline-block; margin-left: 8px; padding: 2px 10px; border: 1px solid currentColor; border-radius: var(--radius-sm); ",
    "background: none; color: inherit; cursor: pointer; font-size: 12px; font-weight: 600; opacity: 0.8; vertical-align: baseline; }",
    ".toast-action:hover { opacity: 1; background: rgba(0, 0, 0, 0.1); }",

    // Toast slide animations.
    "@keyframes toastIn { from { transform: translateX(120%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }",
    "@keyframes toastOut { from { transform: translateX(0); opacity: 1; } to { transform: translateX(120%); opacity: 0; } }",

    // Responsive: full-width toasts on narrow screens.
    "@media (max-width: 768px) { .toast-container { left: 20px; right: 20px; } .toast { min-width: 0; max-width: none; } }",

    // Loading state for buttons.
    ".btn.loading { opacity: 0.7; pointer-events: none; }",
    ".btn.loading::after { content: '...'; }",

    // Inline copy button for Quick Start section.
    ".btn-copy-inline { background: var(--surface-elevated); border: 1px solid var(--border-default); padding: 2px 8px; font-size: 12px; ",
    "border-radius: var(--radius-sm); cursor: pointer; color: var(--text-secondary); margin-left: 6px; vertical-align: middle; }",
    ".btn-copy-inline:hover { background: var(--surface-hover); color: var(--text-primary); }",
    ".copy-feedback-inline { color: var(--stream-healthy); font-size: 12px; margin-left: 8px; display: none; }",

    // Version display styles.
    ".version-container { display: inline-flex; align-items: center; gap: 6px; }",
    ".version { font-size: 13px; color: var(--text-muted); font-weight: 400; text-decoration: none; transition: color 0.2s; }",
    ".version:hover { color: var(--text-primary); }",
    ".version.version-update { color: var(--interactive-primary); }",
    ".version.version-update:hover { color: var(--interactive-primary-hover, var(--interactive-primary)); text-decoration: underline; }",
    ".version-check { background: none; border: none; padding: 0; margin: 0; cursor: pointer; font-size: 14px; color: var(--text-muted); ",
    "line-height: 1; transition: color 0.2s, transform 0.3s; opacity: 0.7; }",
    ".version-check:hover { color: var(--text-primary); opacity: 1; }",
    ".version-check.checking { animation: spin 1s linear infinite; pointer-events: none; }",
    ".version-check.up-to-date { color: var(--stream-healthy); opacity: 1; }",
    ".version-check.check-error { color: var(--stream-error); opacity: 1; }",
    "@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }",

    // Changelog modal styles.
    ".changelog-modal { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0, 0, 0, 0.5); display: none; ",
    "align-items: center; justify-content: center; z-index: 1000; }",
    ".changelog-modal-content { background: var(--surface-overlay); padding: 30px; border-radius: var(--radius-lg); max-width: 500px; width: 90%; ",
    "box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3); }",
    ".changelog-modal-content h3 { margin-top: 0; margin-bottom: 20px; color: var(--text-heading); }",
    ".changelog-list { margin: 0 0 20px 0; padding: 0 0 0 20px; color: var(--text-secondary); font-size: 14px; line-height: 1.6; }",
    ".changelog-list li { margin-bottom: 8px; }",
    ".changelog-list li:last-child { margin-bottom: 0; }",
    ".changelog-modal-buttons { display: flex; gap: 12px; justify-content: flex-end; }"
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

    // Count the number of video channels (excluding static pages).
    const channels = getAllChannels();

    const videoChannelCount = Object.keys(channels).filter((name) => {

      const channel = channels[name];
      const profile = resolveProfile(channel.profile);

      return !profile.noVideo;
    }).length;

    // Generate content for each tab.
    const overviewContent = generateOverviewContent(baseUrl, videoChannelCount);
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
      generateConfigSubtabScript(),
      generateStatusScript()
    ].join("\n");

    // Build and send the complete page.
    const html = generatePageWrapper("PrismCast", styles, bodyContent, scripts);

    res.send(html);
  });
}
