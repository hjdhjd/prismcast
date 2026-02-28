/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * root.ts: Landing page route for PrismCast.
 */
import type { Express, Request, Response } from "express";
import { OPTIONAL_COLUMNS, generateAdvancedTabContent, generateChannelsPanel, generateProvidersPanel, generateSettingsFormFooter, generateSettingsTabContent,
  generateWizardModal, hasEnvOverrides } from "./config.js";
import { checkForUpdates, escapeHtml, getChangelogItems, getPackageVersion, getVersionInfo, isRunningAsService } from "../utils/index.js";
import { generateBaseStyles, generatePageWrapper, generateTabButton, generateTabPanel, generateTabScript, generateTabStyles } from "./ui.js";
import { VIDEO_QUALITY_PRESETS } from "../config/presets.js";
import { getUITabs } from "../config/userConfig.js";
import { resolveBaseUrl } from "./playlist.js";

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
    "    var showSuffix = s.showName ? ' <span class=\"stream-popover-show\">' + s.showName + '</span>' : '';",
    "    html += '<div class=\"stream-popover-row\">';",
    "    html += '<span class=\"status-dot\" style=\"color: ' + color + ';\">&#9679;</span>';",
    "    html += channelDisplayHtml(s.logoUrl, name, 'stream-popover-logo', 'stream-popover-channel');",
    "    html += showSuffix;",
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

    // Real-time health indicator updates. These functions update channel health icons and provider login icons in the Channels tab without a page refresh.
    "function formatTimeAgo(ts) {",
    "  var seconds = Math.floor((Date.now() - ts) / 1000);",
    "  if(seconds < 60) { return 'just now'; }",
    "  var minutes = Math.floor(seconds / 60);",
    "  if(minutes < 60) { return minutes + (minutes === 1 ? ' minute ago' : ' minutes ago'); }",
    "  var hours = Math.floor(minutes / 60);",
    "  if(hours < 24) { return hours + (hours === 1 ? ' hour ago' : ' hours ago'); }",
    "  var days = Math.floor(hours / 24);",
    "  return days + (days === 1 ? ' day ago' : ' days ago');",
    "}",
    "function updateChannelHealth(channelKey, status, timestamp, providerTag) {",
    "  var row = document.getElementById('display-row-' + channelKey);",
    "  if(!row) { return; }",

    // If a providerTag was supplied (snapshot and real-time events both include it), verify it matches the currently selected provider for this channel. The login
    // button carries a data-provider-tag attribute with the selected provider. If the tags differ the health entry is from a previous provider and should be ignored.
    "  if(providerTag) {",
    "    var loginBtn = row.querySelector('.btn-icon-login');",
    "    if(loginBtn && loginBtn.getAttribute('data-provider-tag') !== providerTag) { return; }",
    "  }",
    "  var icon = row.querySelector('.btn-icon-health');",
    "  if(!icon) { return; }",
    "  icon.classList.remove('health-success', 'health-failed');",
    "  if(status === 'success') { icon.classList.add('health-success'); }",
    "  else if(status === 'failed') { icon.classList.add('health-failed'); }",
    "  icon.title = (status === 'success' ? 'Succeeded ' : 'Failed ') + formatTimeAgo(timestamp);",
    "}",
    "function updateProviderAuth(providerTag, timestamp) {",
    "  var buttons = document.querySelectorAll('.btn-icon-login[data-provider-tag=\"' + providerTag + '\"]');",
    "  for(var i = 0; i < buttons.length; i++) {",
    "    buttons[i].classList.add('health-success');",
    "    buttons[i].title = 'Verified ' + formatTimeAgo(timestamp);",
    "  }",
    "}",
    "function applyHealthSnapshot(data) {",
    "  var key;",
    "  for(key in data.channels) {",
    "    var entry = data.channels[key];",
    "    updateChannelHealth(key, entry.status, entry.timestamp, entry.providerTag);",
    "  }",
    "  for(key in data.providers) {",
    "    updateProviderAuth(key, data.providers[key]);",
    "  }",
    "}",

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
    "    if(data.health) { applyHealthSnapshot(data.health); }",
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
    "  on('healthChanged', function(e) {",
    "    var event = JSON.parse(e.data);",
    "    updateChannelHealth(event.channelKey, event.status, event.timestamp, event.providerTag);",
    "    if(event.status === 'success') { updateProviderAuth(event.providerTag, event.timestamp); }",
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

    // JS-based tooltips for devices where the primary input can't hover (iPadOS). Safari on iPadOS doesn't show native title tooltips, so we use
    // a single <div> appended to <body> and positioned via getBoundingClientRect(). This is immune to overflow containers and stacking contexts.
    // On pure-touch devices without a trackpad, mouseenter never fires so the tooltip stays hidden. Desktop skips initialization entirely.
    "(function() {",
    "  if(!window.matchMedia('(hover: none)').matches) return;",
    "  var tip = document.createElement('div');",
    "  tip.className = 'btn-icon-tooltip';",
    "  document.body.appendChild(tip);",
    "  document.addEventListener('mouseenter', function(e) {",
    "    var btn = e.target.closest('.btn-icon[aria-label]');",
    "    if(!btn) return;",
    "    var label = btn.getAttribute('title') || btn.getAttribute('aria-label');",
    "    if(!label) return;",
    "    var rect = btn.getBoundingClientRect();",
    "    tip.textContent = label;",
    "    tip.classList.add('visible');",
    "    tip.style.top = (rect.bottom + 6) + 'px';",
    "    tip.style.left = (rect.left + rect.width / 2) + 'px';",
    "    tip.style.transform = 'translateX(-50%)';",
    "  }, true);",
    "  document.addEventListener('mouseleave', function(e) {",
    "    var src = e.target.closest('.btn-icon[aria-label]');",
    "    if(!src) return;",
    "    var dest = e.relatedTarget && e.relatedTarget.closest && e.relatedTarget.closest('.btn-icon[aria-label]');",
    "    if(dest === src) return;",
    "    tip.classList.remove('visible');",
    "  }, true);",
    "})();",

    "</script>"
  ].join("\n");
}

/**
 * Generates the Overview tab content with a comprehensive user guide covering what PrismCast is, video quality expectations, quick start instructions, tuning speed,
 * channel authentication, working with channels, and system requirements.
 * @param baseUrl - The base URL for the server.
 * @returns HTML content for the Overview tab.
 */
function generateOverviewContent(baseUrl: string): string {

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
    "<li>Optionally, go to the <a href=\"#channels\">Channels tab</a> and set the <strong>provider filter</strong> to only include streaming services you ",
    "subscribe to. This controls which channels Channels DVR sees in the playlist.</li>",
    "<li>Your configured channels will be imported automatically.</li>",
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
    "<p>Some channels (Comedy Central, Fox, NBC, etc.) are available from multiple streaming providers. The <strong>provider dropdown</strong> on each ",
    "channel lets you choose which service to use for that channel. Different providers may offer different tuning performance.</p>",

    "<h4>Provider Filter</h4>",
    "<p>If you only subscribe to certain streaming services, use the <strong>provider filter</strong> on the ",
    "<a href=\"#channels\">Channels tab</a> toolbar to show only relevant channels. This filter also controls which channels appear in the playlist ",
    "that Channels DVR imports &mdash; set it before adding the playlist source in the <a href=\"#overview\">Quick Start</a>. You can also filter ",
    "programmatically using the <code>?provider=</code> query parameter on the playlist URL.</p>",

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
    "<div class=\"api-index\">",

    "<div class=\"api-index-group\">",
    "<a href=\"#api-streaming\" class=\"api-index-heading\">Streaming</a>",
    "<span class=\"api-index-desc\">HLS and MPEG-TS video streams.</span>",
    "<a href=\"#api-streaming\"><code>GET /hls/:name/stream.m3u8</code></a>",
    "<a href=\"#api-streaming\"><code>GET /stream/:name</code></a>",
    "<a href=\"#api-streaming\"><code>GET /play</code></a>",
    "</div>",

    "<div class=\"api-index-group\">",
    "<a href=\"#api-playlist\" class=\"api-index-heading\">Playlist</a>",
    "<span class=\"api-index-desc\">M3U playlist for Channels DVR.</span>",
    "<a href=\"#api-playlist\"><code>GET /playlist</code></a>",
    "</div>",

    "<div class=\"api-index-group\">",
    "<a href=\"#api-channels\" class=\"api-index-heading\">Channels</a>",
    "<span class=\"api-index-desc\">Add, edit, import, and toggle channel definitions.</span>",
    "<a href=\"#api-channels\"><code>POST /config/channels</code></a>",
    "<a href=\"#api-channels\"><code>GET /config/channels/export</code></a>",
    "<a href=\"#api-channels\"><code>POST /config/channels/import</code></a>",
    "</div>",

    "<div class=\"api-index-group\">",
    "<a href=\"#api-providers\" class=\"api-index-heading\">Providers</a>",
    "<span class=\"api-index-desc\">Channel discovery, provider selection, and playlist filtering.</span>",
    "<a href=\"#api-providers\"><code>GET /providers/:slug/channels</code></a>",
    "<a href=\"#api-providers\"><code>POST /config/provider</code></a>",
    "<a href=\"#api-providers\"><code>POST /config/provider-filter</code></a>",
    "</div>",

    "<div class=\"api-index-group\">",
    "<a href=\"#api-auth\" class=\"api-index-heading\">Authentication</a>",
    "<span class=\"api-index-desc\">TV provider login sessions.</span>",
    "<a href=\"#api-auth\"><code>POST /auth/login</code></a>",
    "<a href=\"#api-auth\"><code>POST /auth/done</code></a>",
    "</div>",

    "<div class=\"api-index-group\">",
    "<a href=\"#api-management\" class=\"api-index-heading\">Management</a>",
    "<span class=\"api-index-desc\">List channels, view and control active streams.</span>",
    "<a href=\"#api-management\"><code>GET /channels</code></a>",
    "<a href=\"#api-management\"><code>GET /streams</code></a>",
    "<a href=\"#api-management\"><code>DELETE /streams/:id</code></a>",
    "</div>",

    "<div class=\"api-index-group\">",
    "<a href=\"#api-settings\" class=\"api-index-heading\">Settings</a>",
    "<span class=\"api-index-desc\">Save, export, and import server configuration.</span>",
    "<a href=\"#api-settings\"><code>POST /config</code></a>",
    "<a href=\"#api-settings\"><code>GET /config/export</code></a>",
    "<a href=\"#api-settings\"><code>POST /config/import</code></a>",
    "</div>",

    "<div class=\"api-index-group\">",
    "<a href=\"#api-diagnostics\" class=\"api-index-heading\">Diagnostics</a>",
    "<span class=\"api-index-desc\">Health checks, logs, and real-time monitoring.</span>",
    "<a href=\"#api-diagnostics\"><code>GET /health</code></a>",
    "<a href=\"#api-diagnostics\"><code>GET /logs</code></a>",
    "<a href=\"#api-diagnostics\"><code>GET /logs/stream</code></a>",
    "</div>",

    "</div>",
    "</div>",

    // Streaming endpoints.
    "<div class=\"section\">",
    "<h3 id=\"api-streaming\">Streaming</h3>",
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
    "<h3 id=\"api-playlist\">Playlist</h3>",
    "<table>",
    "<tr><th style=\"width: 35%;\">Endpoint</th><th>Description</th></tr>",
    "<tr>",
    "<td class=\"endpoint\"><a href=\"/playlist\"><code>GET /playlist</code></a></td>",
    "<td>M3U playlist of all channels in Channels DVR format. Use this URL when adding PrismCast as a custom channel source. " +
    "Optional query parameters: " +
    "<code>?provider=</code> filters by streaming provider (<code>?provider=yttv</code>, <code>?provider=yttv,sling</code>, " +
    "<code>?provider=-hulu</code>). " +
    "<code>?sort=</code> overrides sort field (<code>name</code>, <code>key</code>, <code>channelNumber</code>, <code>provider</code>, " +
    "<code>profile</code>, <code>stationId</code>, <code>channelSelector</code>). " +
    "<code>?direction=</code> overrides sort direction (<code>asc</code> or <code>desc</code>). " +
    "All parameters are optional and can be combined. " +
    "<strong>Provider filter only controls which channels appear in the playlist, not which provider is used for tuning.</strong></td>",
    "</tr>",
    "</table>",
    "</div>",

    // Channel endpoints.
    "<div class=\"section\">",
    "<h3 id=\"api-channels\">Channels</h3>",
    "<p>Channel definitions, import/export, and predefined channel management.</p>",
    "<table>",
    "<tr><th style=\"width: 35%;\">Endpoint</th><th>Description</th></tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>POST /config/channels</code></td>",
    "<td>Add, edit, delete, or revert user channels. Body includes <code>action</code> (add/edit/delete/revert) and channel data. " +
    "Revert removes a predefined channel override, restoring defaults.</td>",
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
    "<td class=\"endpoint\"><code>POST /config/channels/bulk-toggle-predefined</code></td>",
    "<td>Enable or disable predefined channels by scope. Body: <code>{ \"enabled\": true, \"scope\": \"all\" | \"pacific\" | \"east\" }</code></td>",
    "</tr>",
    "</table>",
    "</div>",

    // Provider endpoints.
    "<div class=\"section\">",
    "<h3 id=\"api-providers\">Providers</h3>",
    "<p>Channel discovery, provider selection, and filtering for multi-provider channels.</p>",
    "<table>",
    "<tr><th style=\"width: 35%;\">Endpoint</th><th>Description</th></tr>",
    "<tr>",
    "<td class=\"endpoint\"><code>GET /providers/:slug/channels</code></td>",
    "<td>Discover all available channels for a provider. Returns a JSON array of channel objects with <code>name</code>, <code>channelSelector</code>, " +
    "and optional <code>affiliate</code> and <code>tier</code> fields. Provider slugs: <code>foxcom</code>, <code>hbomax</code>, <code>hulu</code>, " +
    "<code>sling</code>, <code>yttv</code>. Returns cached results instantly when a prior tune or discovery call has already enumerated the lineup. " +
    "Add <code>?refresh=true</code> to clear caches and force a fresh discovery walk.</td>",
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

    // Authentication endpoints.
    "<div class=\"section\">",
    "<h3 id=\"api-auth\">Authentication</h3>",
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

    // Management endpoints.
    "<div class=\"section\">",
    "<h3 id=\"api-management\">Management</h3>",
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

    // Settings endpoints.
    "<div class=\"section\">",
    "<h3 id=\"api-settings\">Settings</h3>",
    "<p>Server configuration and backup.</p>",
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
    "</table>",
    "</div>",

    // Diagnostics endpoints.
    "<div class=\"section\">",
    "<h3 id=\"api-diagnostics\">Diagnostics</h3>",
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

    // Channels subtab bar. Uses scoped CSS classes to avoid collision with Configuration tab subtabs.
    "<div class=\"channels-subtab-bar\" role=\"tablist\">",
    "<button type=\"button\" class=\"channels-subtab-btn active\" data-channels-subtab=\"channels\" role=\"tab\" aria-selected=\"true\">Channels</button>",
    "<button type=\"button\" class=\"channels-subtab-btn\" data-channels-subtab=\"providers\" role=\"tab\" aria-selected=\"false\">Providers</button>",
    "</div>",

    // Channels subtab panel (default active).
    "<div id=\"channels-subtab-channels\" class=\"channels-subtab-panel active\">",
    "<div class=\"section\">",
    generateChannelsPanel(),
    "</div>",
    "</div>",

    // Providers subtab panel.
    "<div id=\"channels-subtab-providers\" class=\"channels-subtab-panel\">",
    "<div class=\"section\">",
    generateProvidersPanel(),
    "</div>",
    "</div>",

    // Profile builder wizard modal.
    generateWizardModal(),

    // Test flow dialog. Shown when the user starts a profile test to check selectors against a live page.
    "<div id=\"test-modal\" class=\"login-modal\" style=\"display: none;\">",
    "<div class=\"login-modal-content\">",
    "<h3>Profile Test</h3>",
    "<p id=\"test-modal-status\">A Chrome window has been opened on the PrismCast server. Navigate to the site and verify that your profile works.</p>",
    "<div id=\"test-selector-results\" style=\"display: none;\"></div>",
    "<div class=\"login-modal-buttons\" style=\"gap: 8px;\">",
    "<button type=\"button\" class=\"btn btn-secondary\" id=\"test-check-btn\" onclick=\"checkSelectors()\">Check Selectors</button>",
    "<button type=\"button\" class=\"btn btn-primary\" onclick=\"endProfileTest()\">Done</button>",
    "</div>",
    "</div>",
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
    "    closeBtn.setAttribute('aria-label', 'Dismiss');",
    "    closeBtn.onclick = function() { dismissToast(toast); };",
    "    toast.appendChild(closeBtn);",
    "    container.appendChild(toast);",
    "    var ms = duration !== undefined ? duration : type === 'error' ? 0 : type === 'warning' ? 8000 : 5000;",
    "    if (ms > 0) { setTimeout(function() { dismissToast(toast); }, ms); }",
    "  }",

    // Expose showToast globally so other script blocks (e.g., channels-subtab IIFE) can use the toast system.
    "  window.showToast = showToast;",

    // Dismiss a toast with slide-out animation.
    "  function dismissToast(toast) {",
    "    if (toast.classList.contains('toast-exit')) return;",
    "    toast.classList.add('toast-exit');",
    "    toast.addEventListener('animationend', function() { if (toast.parentNode) toast.parentNode.removeChild(toast); });",
    "  }",

    // Hint appended to success toasts when a channel operation changes M3U playlist content that Channels DVR consumes.
    "  var PLAYLIST_HINT = ' Reload the playlist in Channels DVR to see this change.';",

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
    "    if (input.hasAttribute('data-checkbox-list')) {",
    "      var values = JSON.parse(value || '[]');",
    "      var grid = input.closest('.form-group').querySelector('.checkbox-list-grid');",
    "      if (grid) {",
    "        var cbs = grid.querySelectorAll('input[type=\"checkbox\"]');",
    "        for (var k = 0; k < cbs.length; k++) {",
    "          cbs[k].checked = values.indexOf(cbs[k].value) !== -1;",
    "        }",
    "      }",
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
    "          btn.setAttribute('aria-label', 'Reset to default');",
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
    "              showToastAfterReload('Channels imported successfully.' + PLAYLIST_HINT, 'success');",
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
    "          if (data.imported > 0 || data.replaced > 0) { showToastAfterReload(msg + PLAYLIST_HINT, 'success'); }",
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
    // Uses the current table sort field and direction for correct insertion order.
    "  window.insertChannelRow = function(html, key) {",
    "    var tbody = document.querySelector('.channel-table tbody');",
    "    var table = document.querySelector('.channel-table');",
    "    if (!tbody || !html || !table) return;",
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
    // Find insertion point using current sort field and direction.
    "    var sortField = table.getAttribute('data-sort-field') || 'name';",
    "    var sortDir = table.getAttribute('data-sort-dir') || 'asc';",
    "    var newVal = getSortValue(newDisplayRow, sortField);",
    "    var rows = tbody.querySelectorAll('tr[id^=\"display-row-\"]');",
    "    var inserted = false;",
    "    for (var i = 0; i < rows.length; i++) {",
    "      var rowVal = getSortValue(rows[i], sortField);",
    "      var cmp = (sortDir === 'asc') ? (newVal < rowVal) : (newVal > rowVal);",
    "      if (cmp) {",
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
    "          refilterChannelRows();",
    "        }",
    "        if (action === 'add') {",
    "          hideAddForm();",
    "        } else {",
    "          hideEditForm(result.data.key);",
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
    "          refilterChannelRows();",
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
    "        showToast('Channel ' + key + ' ' + (enable ? 'enabled' : 'disabled') + '.' + PLAYLIST_HINT, 'success');",
    "        updateChannelRowDisabledState(key, !enable);",
    "      } else {",
    "        showToast(result.error || 'Failed to toggle channel.', 'error');",
    "      }",
    "    })",
    "    .catch(function(err) { showToast('Failed to toggle channel: ' + err.message, 'error'); });",
    "  };",

    // Update a channel row's provider selection in-place. Syncs the HTML selected attribute so filterChannelRows() restore logic works correctly. We iterate
    // _allOptions (if present) rather than querySelectorAll because filtered-out options are removed from the DOM but still tracked in the array.
    "  function updateChannelProviderUI(channelKey, variant) {",
    "    var row = document.getElementById('display-row-' + channelKey);",
    "    if (!row) return;",
    "    var sel = row.querySelector('.provider-select');",
    "    if (!sel) return;",
    "    sel.value = variant;",
    "    var allOpts = sel._allOptions || Array.prototype.slice.call(sel.querySelectorAll('option'));",
    "    for (var oi = 0; oi < allOpts.length; oi++) {",
    "      if (allOpts[oi].value === variant) { allOpts[oi].setAttribute('selected', ''); }",
    "      else { allOpts[oi].removeAttribute('selected'); }",
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
    "        if (result.html) { insertChannelRow(result.html, channelKey); refilterChannelRows(); }",
    "      } else {",
    "        showToast(result.error || 'Failed to update provider.', 'error');",
    "      }",
    "    })",
    "    .catch(function(err) { showToast('Failed to update provider: ' + err.message, 'error'); });",
    "  };",

    // Bulk toggle predefined channels by scope (all, pacific, east).
    "  window.bulkTogglePredefined = function(enable, scope) {",
    "    fetch('/config/channels/bulk-toggle-predefined', {",
    "      method: 'POST',",
    "      headers: { 'Content-Type': 'application/json' },",
    "      body: JSON.stringify({ enabled: enable, scope: scope })",
    "    })",
    "    .then(function(response) { return response.json(); })",
    "    .then(function(result) {",
    "      if (result.success) {",
    "        var labels = { all: 'All predefined', pacific: 'Pacific predefined', east: 'East predefined' };",
    "        showToast((labels[scope] || 'Predefined') + ' channels ' + (enable ? 'enabled' : 'disabled') + '.' + PLAYLIST_HINT, 'success');",
    "        if (result.keys) {",
    "          for (var i = 0; i < result.keys.length; i++) { setRowDisabledState(result.keys[i], !enable); }",
    "        }",
    "        updateDisabledCount();",
    "      } else {",
    "        showToast(result.error || 'Failed to toggle channels.', 'error');",
    "      }",
    "    })",
    "    .catch(function(err) { showToast('Failed to toggle channels: ' + err.message, 'error'); });",
    "  };",

    // SVG icon strings for dynamic DOM manipulation when toggling button states.
    "  var ICON_LOGIN_SVG = '<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" " +
    "stroke-linecap=\"round\" stroke-linejoin=\"round\"><path d=\"M6.5 2H3.5a1 1 0 00-1 1v10a1 1 0 001 1h3\"/><path d=\"M10.5 11l3-3-3-3\"/>" +
    "<path d=\"M13.5 8H6.5\"/></svg>';",
    "  var ICON_ENABLE_SVG = '<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" " +
    "stroke-linecap=\"round\" stroke-linejoin=\"round\"><circle cx=\"8\" cy=\"8\" r=\"6\"/><path d=\"M5.5 8l2 2 3.5-4\"/></svg>';",
    "  var ICON_DISABLE_SVG = '<svg width=\"14\" height=\"14\" viewBox=\"0 0 16 16\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\" " +
    "stroke-linecap=\"round\" stroke-linejoin=\"round\"><circle cx=\"8\" cy=\"8\" r=\"6\"/><path d=\"M5.5 5.5l5 5\"/></svg>';",

    // Revert a channel override back to predefined defaults.
    "  window.revertChannel = function(key) {",
    "    if (!confirm('Revert channel ' + key + ' to predefined defaults?')) return;",
    "    fetch('/config/channels', {",
    "      method: 'POST',",
    "      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },",
    "      body: JSON.stringify({ action: 'revert', key: key })",
    "    })",
    "    .then(function(response) { return response.json().then(function(d) { return { ok: response.ok, data: d }; }); })",
    "    .then(function(result) {",
    "      if (result.ok && result.data.success) {",
    "        showToast(result.data.message, 'success');",
    "        if (result.data.html) {",
    "          insertChannelRow(result.data.html, result.data.key || key);",
    "          refilterChannelRows();",
    "        }",
    "      } else {",
    "        showToast(result.data.message || 'Failed to revert channel.', 'error');",
    "      }",
    "    })",
    "    .catch(function(err) { showToast('Failed to revert channel: ' + err.message, 'error'); });",
    "  };",

    // Set a channel row's disabled state without triggering count updates. Uses icon buttons. Used by bulkTogglePredefined for efficient bulk updates.
    "  function setRowDisabledState(key, disabled) {",
    "    var row = document.getElementById('display-row-' + key);",
    "    if (!row) return;",
    "    var btnGroup = row.querySelector('.btn-group');",
    "    if (!btnGroup) return;",
    "    if (disabled) {",
    "      row.classList.add('channel-disabled');",
    "      row.classList.remove('user-channel');",
    // Replace login icon with placeholder.
    "      var loginBtn = btnGroup.querySelector('.btn-icon-login');",
    "      if (loginBtn) {",
    "        var placeholder = document.createElement('span');",
    "        placeholder.className = 'btn-icon-placeholder';",
    "        loginBtn.replaceWith(placeholder);",
    "      }",
    // Swap disable icon to enable icon.
    "      var disableBtn = btnGroup.querySelector('.btn-icon-disable');",
    "      if (disableBtn) {",
    "        disableBtn.className = 'btn-icon btn-icon-enable';",
    "        disableBtn.title = 'Enable';",
    "        disableBtn.setAttribute('aria-label', 'Enable');",
    "        disableBtn.innerHTML = ICON_ENABLE_SVG;",
    "        disableBtn.setAttribute('onclick', \"togglePredefinedChannel('\" + key + \"', true)\");",
    "      }",
    "    } else {",
    "      row.classList.remove('channel-disabled');",
    // Swap enable icon to disable icon.
    "      var enableBtn = btnGroup.querySelector('.btn-icon-enable');",
    "      if (enableBtn) {",
    // Replace placeholder with login icon button.
    "        var placeholder = btnGroup.querySelector('.btn-icon-placeholder');",
    "        if (placeholder) {",
    "          var newLoginBtn = document.createElement('button');",
    "          newLoginBtn.type = 'button';",
    "          newLoginBtn.className = 'btn-icon btn-icon-login';",
    "          newLoginBtn.title = 'Login';",
    "          newLoginBtn.setAttribute('aria-label', 'Login');",
    "          newLoginBtn.innerHTML = ICON_LOGIN_SVG;",
    "          newLoginBtn.setAttribute('onclick', \"startChannelLogin('\" + key + \"')\");",
    "          placeholder.replaceWith(newLoginBtn);",
    "        }",
    "        enableBtn.className = 'btn-icon btn-icon-disable';",
    "        enableBtn.title = 'Disable';",
    "        enableBtn.setAttribute('aria-label', 'Disable');",
    "        enableBtn.innerHTML = ICON_DISABLE_SVG;",
    "        enableBtn.setAttribute('onclick', \"togglePredefinedChannel('\" + key + \"', false)\");",
    "      }",
    "    }",
    "  }",

    // Update a single channel row's disabled state and refresh counts. Used by individual togglePredefinedChannel.
    "  function updateChannelRowDisabledState(key, disabled) {",
    "    setRowDisabledState(key, disabled);",
    "    updateDisabledCount();",
    "  };",

    // Update the disabled channel count shown in the toolbar toggle label. Uses a union selector to avoid double-counting rows that are both disabled and
    // provider-unavailable.
    "  function updateDisabledCount() {",
    "    var countEl = document.getElementById('disabled-count');",
    "    if (!countEl) return;",
    "    var hiddenRows = document.querySelectorAll('tr.channel-disabled:not(.user-channel), tr.channel-unavailable');",
    "    countEl.textContent = String(hiddenRows.length);",
    "  };",

    // Save the provider filter to the server and update the UI on success.
    "  function saveProviderFilter(enabledTags) {",
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
    "        showToast('Provider filter updated.' + PLAYLIST_HINT, 'success');",
    "      }",
    "    })",
    "    .catch(function(err) { showToast('Failed to update filter: ' + err.message, 'error'); });",
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
    "    saveProviderFilter(enabledTags);",
    "  };",

    // Remove a provider chip (uncheck the tag and update). Orphaned tags (no checkbox in the dropdown) are removed by rebuilding the list from the remaining chips.
    "  window.removeProviderChip = function(tag) {",
    "    var menu = document.querySelector('.provider-dropdown-menu');",
    "    if (!menu) return;",
    "    var cb = menu.querySelector('input[data-tag=\"' + tag + '\"]');",
    "    if (cb) { cb.checked = false; toggleProviderTag(cb); return; }",

    // No checkbox exists for this tag — it is an orphaned entry with no corresponding provider. Build the enabled list from the remaining chips and POST directly.
    "    var chips = document.querySelectorAll('#provider-chips .provider-chip[data-tag]');",
    "    var enabledTags = [];",
    "    for (var i = 0; i < chips.length; i++) {",
    "      var ct = chips[i].getAttribute('data-tag');",
    "      if (ct && ct !== tag) enabledTags.push(ct);",
    "    }",
    "    saveProviderFilter(enabledTags);",
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
    "      chip.innerHTML = label + '<button type=\"button\" class=\"chip-close\" aria-label=\"Remove ' + label +",
    "        '\" onclick=\"removeProviderChip(\\'' + tag + '\\')\">\\u00d7</button>';",
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

    // Re-run the provider filter on all channel rows using the current checkbox state. Called after insertChannelRow replaces a row (which loses the filter state).
    "  function refilterChannelRows() {",
    "    var menu = document.querySelector('.provider-dropdown-menu');",
    "    if (!menu) return;",
    "    var cbs = menu.querySelectorAll('input[type=\"checkbox\"]:not(:disabled)');",
    "    var enabledTags = [];",
    "    var allChecked = true;",
    "    for (var i = 0; i < cbs.length; i++) {",
    "      if (cbs[i].checked) { enabledTags.push(cbs[i].getAttribute('data-tag')); }",
    "      else { allChecked = false; }",
    "    }",
    "    if (allChecked) { enabledTags = []; }",
    "    if (enabledTags.length > 0) { filterChannelRows(enabledTags); }",
    "  }",

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
    "            updateChannelProviderUI(key, sel.variant);",
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
    "            updateChannelProviderUI(key, sel.variant);",
    "          }",
    "        }",
    "      } else {",
    "        showToast(result.error || 'Failed to revert.', 'error');",
    "      }",
    "    })",
    "    .catch(function(err) { showToast('Failed to revert: ' + err.message, 'error'); });",
    "  }",

    // Close all open dropdown menus and remove the scroll and resize listeners.
    "  function closeDropdowns() {",
    "    var menus = document.querySelectorAll('.dropdown-menu.show');",
    "    for (var i = 0; i < menus.length; i++) menus[i].classList.remove('show');",
    "    window.removeEventListener('scroll', closeDropdowns, true);",
    "    window.removeEventListener('resize', closeDropdowns);",
    "  };",
    "  window.closeDropdowns = closeDropdowns;",

    // Toggle a dropdown menu open or closed. On first call for a given button, the menu is detached from its .dropdown parent and portaled to <body> with
    // position: fixed. This allows the menu to escape overflow: auto containers like the channel table wrapper. On every open, the menu is positioned relative
    // to the button's bounding rect, with edge-of-viewport clamping and above-button flip when it would extend below the viewport.
    "  window.toggleDropdown = function(btn) {",
    "    var menu = btn._portalMenu;",
    "    var isOpen = menu && menu.classList.contains('show');",
    "    closeDropdowns();",
    "    if (isOpen) return;",
    "    if (!menu) {",
    "      menu = btn.nextElementSibling;",
    "      if (!menu) return;",
    "      btn._portalMenu = menu;",
    "      document.body.appendChild(menu);",
    "      menu.style.position = 'fixed';",
    "      menu.style.marginTop = '0';",
    "    }",
    "    menu.classList.add('show');",
    "    var rect = btn.getBoundingClientRect();",
    "    var top = rect.bottom + 2;",
    "    var left = rect.left;",
    "    if (left + menu.offsetWidth > window.innerWidth - 4) left = rect.right - menu.offsetWidth;",
    "    if (left < 4) left = 4;",
    "    if (top + menu.offsetHeight > window.innerHeight - 4) top = rect.top - menu.offsetHeight - 2;",
    "    menu.style.top = top + 'px';",
    "    menu.style.left = left + 'px';",
    "    window.addEventListener('scroll', closeDropdowns, true);",
    "    window.addEventListener('resize', closeDropdowns);",
    "  };",

    // Close dropdowns when clicking outside. After portaling, menus live on <body> outside .dropdown containers, so we also check .dropdown-menu to prevent
    // clicks on menu items (like column picker checkboxes) from triggering close.
    "  document.addEventListener('click', function(e) {",
    "    if (!e.target.closest('.dropdown') && !e.target.closest('.dropdown-menu')) closeDropdowns();",
    "  });",

    // Copy a stream URL to the clipboard and show a toast notification. The type parameter selects HLS or MPEG-TS format. Uses the modern Clipboard API when
    // available (secure contexts), falling back to execCommand for plain HTTP access via IP address.
    "  window.copyStreamUrl = function(type, key) {",
    "    closeDropdowns();",
    "    var url = (type === 'hls') ? (location.origin + '/hls/' + key + '/stream.m3u8') : (location.origin + '/stream/' + key);",
    "    if (navigator.clipboard && navigator.clipboard.writeText) {",
    "      navigator.clipboard.writeText(url).then(function() { showToast('Stream URL copied to clipboard.', 'success'); })",
    "        .catch(function() { showToast('Failed to copy URL.', 'error'); });",
    "    } else {",
    "      var ta = document.createElement('textarea');",
    "      ta.value = url;",
    "      ta.style.position = 'fixed';",
    "      ta.style.opacity = '0';",
    "      document.body.appendChild(ta);",
    "      ta.select();",
    "      try { document.execCommand('copy'); showToast('Stream URL copied to clipboard.', 'success'); }",
    "      catch(e) { showToast('Failed to copy URL.', 'error'); }",
    "      document.body.removeChild(ta);",
    "    }",
    "  };",

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

    // Toggle visibility of an optional column and persist via background POST.
    "  window.toggleColumn = function(checkbox) {",
    "    var table = document.querySelector('.channel-table');",
    "    if (!table || !checkbox) return;",
    "    var colClass = checkbox.getAttribute('data-col-class');",
    "    var hideClass = 'hide-' + colClass;",
    "    if (checkbox.checked) { table.classList.remove(hideClass); }",
    "    else { table.classList.add(hideClass); }",
    // Collect all currently checked columns from the picker menu.
    "    var menu = document.querySelector('.column-picker-menu');",
    "    var cols = [];",
    "    if (menu) {",
    "      var cbs = menu.querySelectorAll('input[type=\"checkbox\"]');",
    "      for (var i = 0; i < cbs.length; i++) {",
    "        if (cbs[i].checked) cols.push(cbs[i].getAttribute('data-col-field'));",
    "      }",
    "    }",
    "    fetch('/config/channels/display-prefs', {",
    "      method: 'POST', headers: { 'Content-Type': 'application/json' },",
    "      body: JSON.stringify({ visibleColumns: cols })",
    "    }).catch(function() {});",
    "  };",

    // Read the server-stamped sort value from the data-sort-value attribute.
    "  function getSortValue(row, field) {",
    "    var colIndex = { key: 0, name: 1, provider: 2, channelNumber: 3, stationId: 4, profile: 5, channelSelector: 6 };",
    "    var idx = colIndex[field];",
    "    if (idx === undefined) return '';",
    "    var cell = row.children[idx];",
    "    if (!cell) return '';",
    "    return cell.getAttribute('data-sort-value') || '';",
    "  }",

    // Sort the channel table by the specified field. Toggles direction if the same field is clicked again.
    "  window.sortChannelTable = function(field) {",
    "    var table = document.querySelector('.channel-table');",
    "    if (!table) return;",
    "    var currentField = table.getAttribute('data-sort-field');",
    "    var currentDir = table.getAttribute('data-sort-dir') || 'asc';",
    "    var dir = (field === currentField) ? (currentDir === 'asc' ? 'desc' : 'asc') : 'asc';",
    "    table.setAttribute('data-sort-field', field);",
    "    table.setAttribute('data-sort-dir', dir);",
    // Collect display rows and their paired edit rows.
    "    var tbody = table.querySelector('tbody');",
    "    var displayRows = tbody.querySelectorAll('tr[id^=\"display-row-\"]');",
    "    var pairs = [];",
    "    for (var i = 0; i < displayRows.length; i++) {",
    "      var dr = displayRows[i];",
    "      var key = dr.id.replace('display-row-', '');",
    "      var er = document.getElementById('edit-row-' + key);",
    "      var name = dr.children[1] ? dr.children[1].textContent.trim().toLowerCase() : '';",
    "      pairs.push({ displayRow: dr, editRow: er, val: getSortValue(dr, field), name: name });",
    "    }",
    // Sort pairs. The channel name is used as a secondary sort key to stabilize rows with identical primary values. The secondary sort is always ascending
    // regardless of the primary direction so that rows within each group maintain a consistent alphabetical order.
    "    pairs.sort(function(a, b) {",
    "      var cmp = (dir === 'asc') ? a.val.localeCompare(b.val) : b.val.localeCompare(a.val);",
    "      if (cmp !== 0) return cmp;",
    "      return a.name.localeCompare(b.name);",
    "    });",
    // Re-append in sorted order.
    "    for (var j = 0; j < pairs.length; j++) {",
    "      tbody.appendChild(pairs[j].displayRow);",
    "      if (pairs[j].editRow) tbody.appendChild(pairs[j].editRow);",
    "    }",
    // Update header indicators.
    "    var headers = table.querySelectorAll('th.sortable');",
    "    for (var h = 0; h < headers.length; h++) {",
    "      var th = headers[h];",
    "      var hField = th.getAttribute('data-sort-field');",
    "      var label = th.textContent.replace(/[\\u25B2\\u25BC]/g, '').trim();",
    "      if (hField === field) { th.innerHTML = label + (dir === 'asc' ? ' &#9650;' : ' &#9660;'); }",
    "      else { th.textContent = label; }",
    "    }",
    // Persist sort preference.
    "    fetch('/config/channels/display-prefs', {",
    "      method: 'POST', headers: { 'Content-Type': 'application/json' },",
    "      body: JSON.stringify({ sortField: field, sortDirection: dir })",
    "    }).catch(function() {});",
    "  };",

    // Populate channel selector datalist based on the URL field value. First populates from server-rendered data (predefined channels and cached provider
    // discovery), then asynchronously fetches from the provider discovery endpoint to pick up any channels not yet in the static data.
    "  function updateSelectorSuggestions(urlInputId, datalistId) {",
    "    var urlInput = document.getElementById(urlInputId);",
    "    var datalist = document.getElementById(datalistId);",
    "    if (!urlInput || !datalist) return;",
    "    datalist.innerHTML = '';",

    // Find or create the URL hint element. Placed after the URL input's parent form-row so it sits below the field.
    "    var hintId = urlInputId + '-url-hint';",
    "    var hintEl = document.getElementById(hintId);",
    "    if (!hintEl) {",
    "      hintEl = document.createElement('div');",
    "      hintEl.id = hintId;",
    "      hintEl.className = 'hint';",
    "      hintEl.style.marginBottom = '0';",
    "      var formRow = urlInput.closest('.form-row');",
    "      if (formRow) { formRow.parentNode.insertBefore(hintEl, formRow.nextSibling); }",
    "    }",
    "    hintEl.innerHTML = '';",
    "    hintEl.style.display = 'none';",

    "    var hostname;",
    "    var raw = urlInput.value;",
    "    if (raw && !raw.startsWith('http://') && !raw.startsWith('https://')) { raw = 'https://' + raw; }",
    "    try { hostname = new URL(raw).hostname; } catch (e) { return; }",
    "    var alt = hostname.startsWith('www.') ? hostname.substring(4) : 'www.' + hostname;",
    // Resolve the provider hostname. Try exact match first, then www-variant, then base domain match (e.g., sling.com matches watch.sling.com).
    "    var exactMatch = (typeof channelSelectorsByDomain !== 'undefined') && channelSelectorsByDomain[hostname];",
    "    var resolvedHostname = null;",
    "    if ((typeof providerByDomain !== 'undefined') && providerByDomain[hostname]) { resolvedHostname = hostname; }",
    "    else if ((typeof providerByDomain !== 'undefined') && providerByDomain[alt]) { resolvedHostname = alt; }",
    "    else if (typeof providerByDomain !== 'undefined') {",
    "      var suffix = '.' + hostname;",
    "      var domainKeys = Object.keys(providerByDomain);",
    "      for (var d = 0; d < domainKeys.length; d++) {",
    "        if (domainKeys[d].endsWith(suffix)) { resolvedHostname = domainKeys[d]; break; }",
    "      }",
    "    }",
    "    var slug = resolvedHostname ? providerByDomain[resolvedHostname] : null;",
    "    var entries = exactMatch || ((typeof channelSelectorsByDomain !== 'undefined')",
    "      ? (channelSelectorsByDomain[alt] || (resolvedHostname ? channelSelectorsByDomain[resolvedHostname] : null)) : null);",
    "    if (entries) {",
    "      for (var i = 0; i < entries.length; i++) {",
    "        var opt = document.createElement('option');",
    "        opt.value = entries[i].value;",
    "        opt.label = entries[i].label;",
    "        datalist.appendChild(opt);",
    "      }",
    "    }",

    // Show a URL hint when the hostname didn't match exactly and we found a provider via fallback.
    "    if (!exactMatch && slug && (typeof providerGuideUrl !== 'undefined') && providerGuideUrl[slug]) {",
    "      var suggestedUrl = providerGuideUrl[slug];",
    "      var link = document.createElement('a');",
    "      link.href = '#';",
    "      link.textContent = suggestedUrl;",
    "      link.addEventListener('click', function(e) {",
    "        e.preventDefault();",
    "        urlInput.value = suggestedUrl;",
    "        hintEl.innerHTML = '';",
    "        hintEl.style.display = 'none';",
    "        updateSelectorSuggestions(urlInputId, datalistId);",
    "      });",
    "      hintEl.appendChild(document.createTextNode('Did you mean '));",
    "      hintEl.appendChild(link);",
    "      hintEl.appendChild(document.createTextNode('?'));",
    "      hintEl.style.display = '';",
    "    }",

    "    if (!slug) return;",
    "    var snapshotUrl = urlInput.value;",
    "    fetch('/providers/' + slug + '/channels')",
    "      .then(function(r) { return r.ok ? r.json() : null; })",
    "      .then(function(channels) {",
    "        if (!channels || urlInput.value !== snapshotUrl) return;",
    "        var existing = {};",
    "        var opts = datalist.querySelectorAll('option');",
    "        for (var j = 0; j < opts.length; j++) { existing[opts[j].value] = true; }",
    "        for (var k = 0; k < channels.length; k++) {",
    "          if (!existing[channels[k].channelSelector]) {",
    "            var o = document.createElement('option');",
    "            o.value = channels[k].channelSelector;",
    "            o.label = channels[k].name;",
    "            datalist.appendChild(o);",
    "          }",
    "        }",
    "      })",
    "      .catch(function() {});",
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

    // Checkbox list: collect checked values into the hidden input and update the modified indicator. Called by onchange on individual checkboxes within a
    // checkbox-list-grid. The hidden input holds the JSON-encoded array and participates in the standard form submission.
    "  window.updateCheckboxList = function(checkbox) {",
    "    var group = checkbox.closest('.form-group');",
    "    if (!group) return;",
    "    var hidden = group.querySelector('[data-checkbox-list]');",
    "    if (!hidden) return;",
    "    var grid = group.querySelector('.checkbox-list-grid');",
    "    if (!grid) return;",
    "    var cbs = grid.querySelectorAll('input[type=\"checkbox\"]');",
    "    var values = [];",
    "    for (var i = 0; i < cbs.length; i++) {",
    "      if (cbs[i].checked) values.push(cbs[i].value);",
    "    }",
    "    hidden.value = JSON.stringify(values);",
    "    updateModifiedIndicator(hidden);",
    "  };",

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
 * Generates the JavaScript for Channels tab subtab switching (Channels / Providers) and provider management actions. Uses scoped CSS classes and a separate
 * switching function to avoid collision with the Configuration tab's subtab infrastructure.
 * @returns JavaScript code as a string wrapped in script tags.
 */
function generateChannelsSubtabScript(): string {

  return [
    "<script>",
    "(function() {",

    // Channels subtab switching function. Scoped to .channels-subtab-btn / .channels-subtab-panel to avoid colliding with Config tab subtabs.
    "  function switchChannelsSubtab(subtab, updateUrl) {",
    "    var btns = document.querySelectorAll('.channels-subtab-btn');",
    "    var panels = document.querySelectorAll('.channels-subtab-panel');",
    "    for(var i = 0; i < btns.length; i++) {",
    "      btns[i].classList.remove('active');",
    "      btns[i].setAttribute('aria-selected', 'false');",
    "      if(btns[i].getAttribute('data-channels-subtab') === subtab) {",
    "        btns[i].classList.add('active');",
    "        btns[i].setAttribute('aria-selected', 'true');",
    "      }",
    "    }",
    "    for(var j = 0; j < panels.length; j++) {",
    "      panels[j].classList.remove('active');",
    "      if(panels[j].id === 'channels-subtab-' + subtab) {",
    "        panels[j].classList.add('active');",
    "      }",
    "    }",
    "    try { localStorage.setItem('prismcast-channels-subtab', subtab); } catch(e) {}",
    "    if(updateUrl !== false) {",
    "      var newHash = (subtab === 'channels') ? '#channels' : '#channels/' + subtab;",
    "      if(window.location.hash !== newHash) {",
    "        window.location.hash = newHash;",
    "      }",
    "    }",
    "  }",

    // Expose for main tab script hashchange handler.
    "  window.switchChannelsSubtab = switchChannelsSubtab;",

    // Attach click handlers to channels subtab buttons.
    "  var btns = document.querySelectorAll('.channels-subtab-btn');",
    "  for(var i = 0; i < btns.length; i++) {",
    "    btns[i].addEventListener('click', function() {",
    "      switchChannelsSubtab(this.getAttribute('data-channels-subtab'));",
    "    });",
    "  }",

    // Delete a user profile with confirmation.
    "  window.deleteUserProfile = function(key) {",
    "    if(!confirm('Delete profile \"' + key + '\" and all its domain mappings?')) return;",
    "    fetch('/config/profiles/' + encodeURIComponent(key), { method: 'DELETE' })",
    "      .then(function(r) { return r.json(); })",
    "      .then(function(data) {",
    "        if(data.success) { location.reload(); }",
    "        else { showToast('Failed to delete profile: ' + (data.error || 'Unknown error.'), 'error'); }",
    "      })",
    "      .catch(function() { showToast('Failed to delete profile.', 'error'); });",
    "  };",

    // Edit an existing user profile by pre-populating the wizard.
    "  window.editUserProfile = function(key) {",
    "    fetch('/config/profiles')",
    "      .then(function(r) { return r.json(); })",
    "      .then(function(data) {",
    "        if(!data.success) { showToast('Failed to load profiles.', 'error'); return; }",
    "        var profiles = data.profiles || [];",
    "        var match = profiles.find(function(p) { return p.key === key; });",
    "        if(!match || !match.profile) { showToast('Profile not found.', 'error'); return; }",
    "        var p = match.profile;",
    "        var editCs = p.channelSelection || {};",
    "        var strat = editCs.strategy || 'none';",
    "        var strategyFields = {};",
    "        var editStrat = window.__wizardStrategies.find(function(st) { return st.id === strat; });",
    "        if(editStrat) {",
    "          for(var i = 0; i < editStrat.fields.length; i++) {",
    "            var ef = editStrat.fields[i];",
    "            if(ef.type === 'text' && editCs[ef.id]) { strategyFields[ef.id] = editCs[ef.id]; }",
    "            else if(ef.type === 'boolean' && editCs[ef.id]) { strategyFields[ef.id] = true; }",
    "          }",
    "        }",
    "        var flags = {};",
    "        var wizFlags = window.__wizardFlags;",
    "        for(var i = 0; i < wizFlags.length; i++) { if(p[wizFlags[i].id]) flags[wizFlags[i].id] = true; }",
    "        var domains = (match.domains && match.domains.length > 0) ?",
    "          match.domains.map(function(d) { return { domain: d.domain, provider: d.provider || '', providerTag: d.providerTag || '' }; }) :",
    "          [{ domain: '', provider: '', providerTag: '' }];",
    "        wizardState = {",
    "          currentStep: 1, editKey: key, editMode: true, highestStep: 5, profileName: key,",
    "          baseProfile: p.extends || '', strategy: strat,",
    "          strategyFields: strategyFields, flags: flags,",
    "          hideSelector: p.hideSelector || '', domains: domains, description: p.description || ''",
    "        };",
    "        var title = document.getElementById('wizard-title');",
    "        if(title) title.textContent = 'Edit Provider Profile';",
    "        document.getElementById('wizard-modal').style.display = 'flex';",
    "        renderWizardStep();",
    "      })",
    "      .catch(function() { showToast('Failed to load profile data.', 'error'); });",
    "  };",

    // Pending import data stored while the import preview modal is open.
    "  var pendingImportData = null;",

    // Import a provider pack via file picker — shows a preview modal before importing.
    "  window.startProviderImport = function() {",
    "    var input = document.createElement('input');",
    "    input.type = 'file';",
    "    input.accept = '.json,application/json';",
    "    input.onchange = function() {",
    "      if(!input.files || !input.files[0]) return;",
    "      var reader = new FileReader();",
    "      reader.onload = function() {",
    "        try {",
    "          var data = JSON.parse(reader.result);",
    "          pendingImportData = data;",
    "          var profileCount = data.profiles ? Object.keys(data.profiles).length : 0;",
    "          var domainCount = data.domains ? Object.keys(data.domains).length : 0;",
    "          var channelCount = data.channels ? Object.keys(data.channels).length : 0;",
    "          var html = '<table class=\"import-summary-table\">';",
    "          html += '<tr><td class=\"import-summary-label\">Pack name</td>';",
    "          html += '<td>' + esc(data.name || 'Unnamed') + '</td></tr>';",
    "          html += '<tr><td class=\"import-summary-label\">Profiles</td>';",
    "          html += '<td>' + plural(profileCount, 'profile') + '</td></tr>';",
    "          html += '<tr><td class=\"import-summary-label\">Domains</td>';",
    "          html += '<td>' + plural(domainCount, 'domain') + '</td></tr>';",
    "          html += '<tr><td class=\"import-summary-label\">Channels</td>';",
    "          html += '<td>' + plural(channelCount, 'channel') + '</td></tr>';",
    "          html += '</table>';",
    "          if(channelCount > 0) {",
    "            html += '<div class=\"export-divider\"></div>';",
    "            html += '<label class=\"export-option-label\">';",
    "            html += '<input type=\"checkbox\" id=\"import-include-channels\" checked> Include ' + plural(channelCount, 'channel') + '</label>';",
    "            html += '<div class=\"export-hint\">Import the channel definitions from this pack. Uncheck to import only the provider profiles and domains.</div>';",
    "          }",
    "          document.getElementById('import-modal-body').innerHTML = html;",
    "          document.getElementById('import-modal').style.display = 'flex';",
    "        } catch(e) {",
    "          showToast('Invalid JSON file.', 'error');",
    "        }",
    "      };",
    "      reader.readAsText(input.files[0]);",
    "    };",
    "    input.click();",
    "  };",

    // Close the import preview modal.
    "  window.closeImportModal = function() {",
    "    document.getElementById('import-modal').style.display = 'none';",
    "    pendingImportData = null;",
    "  };",

    // Execute the import after the user confirms in the preview modal.
    "  window.executeImport = function() {",
    "    if(!pendingImportData) return;",
    "    var skipChannels = false;",
    "    var cb = document.getElementById('import-include-channels');",
    "    if(cb && !cb.checked) skipChannels = true;",
    "    var body = Object.assign({}, pendingImportData);",
    "    if(skipChannels) body.skipChannels = true;",
    "    fetch('/config/profiles/import', {",
    "      method: 'POST',",
    "      headers: { 'Content-Type': 'application/json' },",
    "      body: JSON.stringify(body)",
    "    })",
    "      .then(function(r) { return r.json(); })",
    "      .then(function(result) {",
    "        closeImportModal();",
    "        if(result.success) {",
    "          var msg = 'Imported ' + plural(result.summary.profilesAdded, 'profile') + ', ' +",
    "            plural(result.summary.domainsAdded, 'domain') + ', ' + plural(result.summary.channelsAdded, 'channel') + '.';",
    "          if(result.warnings && result.warnings.length > 0) msg += ' Warnings: ' + result.warnings.join('; ');",
    "          showToast(msg, 'success');",
    "          location.reload();",
    "        } else {",
    "          showToast('Import failed: ' + (result.error || 'Unknown error.'), 'error');",
    "        }",
    "      })",
    "      .catch(function() { closeImportModal(); showToast('Import request failed.', 'error'); });",
    "  };",

    // Export a provider pack. Opens a modal with profile checklist and options. Shows provider name prominently for each profile with domain and channel counts.
    // The select-all row is hidden when there is only one profile since the single checkbox makes it redundant.
    "  window.startProviderExport = function() {",
    "    fetch('/config/profiles')",
    "      .then(function(r) { return r.json(); })",
    "      .then(function(data) {",
    "        var profiles = data.profiles || [];",
    "        if(profiles.length === 0) { showToast('No user profiles to export.', 'error'); return; }",
    "        var listDiv = document.getElementById('export-profile-list');",
    "        var html = '';",
    "        for(var i = 0; i < profiles.length; i++) {",
    "          var p = profiles[i];",
    "          var domainCount = p.domains ? p.domains.length : 0;",
    "          var channelCount = p.channelCount || 0;",
    "          var providerName = '';",
    "          for(var di = 0; di < (p.domains || []).length; di++) {",
    "            if(p.domains[di].provider) { providerName = p.domains[di].provider; break; }",
    "          }",
    "          var meta = [];",
    "          if(providerName) meta.push(esc(providerName));",
    "          meta.push(plural(domainCount, 'domain'));",
    "          meta.push(plural(channelCount, 'channel'));",
    "          html += '<label class=\"export-profile-item\">';",
    "          html += '<input type=\"checkbox\" class=\"export-profile-cb\" value=\"' + esc(p.key) + '\" checked>';",
    "          html += '<div class=\"export-profile-info\">';",
    "          html += '<div class=\"export-profile-name\">' + esc(p.key) + '</div>';",
    "          html += '<div class=\"export-profile-meta\">' + meta.join(' &middot; ') + '</div>';",
    "          html += '</div></label>';",
    "        }",
    "        listDiv.innerHTML = html;",
    "        document.getElementById('export-select-all').checked = true;",
    "        document.getElementById('export-select-all-row').style.display = (profiles.length > 1) ? '' : 'none';",
    "        document.getElementById('export-include-channels').checked = false;",
    "        document.getElementById('export-modal').style.display = 'flex';",
    "      })",
    "      .catch(function() { showToast('Failed to load profiles.', 'error'); });",
    "  };",

    // Toggle all export profile checkboxes.
    "  window.toggleExportAll = function(master) {",
    "    var cbs = document.querySelectorAll('.export-profile-cb');",
    "    for(var i = 0; i < cbs.length; i++) cbs[i].checked = master.checked;",
    "  };",

    // Close the export modal.
    "  window.closeExportModal = function() {",
    "    document.getElementById('export-modal').style.display = 'none';",
    "  };",

    // Execute the export by building the download URL from the selected profiles.
    "  window.executeExport = function() {",
    "    var cbs = document.querySelectorAll('.export-profile-cb:checked');",
    "    if(cbs.length === 0) { showToast('Select at least one profile.', 'error'); return; }",
    "    var keys = [];",
    "    for(var i = 0; i < cbs.length; i++) keys.push(cbs[i].value);",
    "    var url = '/config/profiles/export?profile=' + encodeURIComponent(keys.join(',')) + '&domains=1';",
    "    if(document.getElementById('export-include-channels').checked) url += '&channels=1';",
    "    closeExportModal();",
    "    window.location.href = url;",
    "  };",

    // Profile builder wizard state and rendering. highestStep tracks the furthest step the user has reached, allowing click-to-navigate back to visited steps.
    "  var wizardState = {",
    "    currentStep: 1,",
    "    editKey: null,",
    "    editMode: false,",
    "    highestStep: 1,",
    "    profileName: '',",
    "    baseProfile: '',",
    "    strategy: 'none',",
    "    strategyFields: {},",
    "    flags: {},",
    "    hideSelector: '',",
    "    domains: [{ domain: '', provider: '', providerTag: '' }],",
    "    description: ''",
    "  };",

    // Auto-generate a provider tag from a provider name: lowercase, strip non-alphanumeric.
    "  function autoProviderTag(name) {",
    "    return name.toLowerCase().replace(/[^a-z0-9]/g, '');",
    "  }",

    // HTML-escape user input for safe innerHTML insertion in the review step.
    "  function esc(str) {",
    "    var d = document.createElement('div');",
    "    d.appendChild(document.createTextNode(str || ''));",
    "    return d.innerHTML;",
    "  }",

    // Pluralization helper for UI text: returns "1 domain", "2 domains", etc.
    "  function plural(n, word) {",
    "    return n + ' ' + word + (n === 1 ? '' : 's');",
    "  }",

    // Render the current wizard step into the wizard-content area.
    "  function renderWizardStep() {",
    "    var content = document.getElementById('wizard-content');",
    "    var errDiv = document.getElementById('wizard-error');",
    "    var backBtn = document.getElementById('wizard-back');",
    "    var nextBtn = document.getElementById('wizard-next');",
    "    var saveBtn = document.getElementById('wizard-save');",
    "    var saveTestBtn = document.getElementById('wizard-save-test');",
    "    if(!content) return;",
    "    errDiv.style.display = 'none';",
    "    errDiv.textContent = '';",

    // Update step indicator. Steps up to highestStep are clickable for navigation.
    "    var steps = document.querySelectorAll('.wizard-step');",
    "    for(var i = 0; i < steps.length; i++) {",
    "      var stepNum = parseInt(steps[i].getAttribute('data-step'), 10);",
    "      steps[i].classList.remove('active', 'completed', 'clickable');",
    "      if(stepNum < wizardState.currentStep) steps[i].classList.add('completed');",
    "      if(stepNum === wizardState.currentStep) steps[i].classList.add('active');",
    "      if(stepNum !== wizardState.currentStep && stepNum <= wizardState.highestStep) steps[i].classList.add('clickable');",
    "      steps[i].onclick = (function(n) { return function() { wizardGoToStep(n); }; })(stepNum);",
    "    }",

    // Show/hide navigation buttons.
    "    backBtn.style.display = (wizardState.currentStep > 1) ? '' : 'none';",
    "    nextBtn.style.display = (wizardState.currentStep < 5) ? '' : 'none';",
    "    saveBtn.style.display = (wizardState.currentStep === 5) ? '' : 'none';",
    "    saveTestBtn.style.display = (wizardState.currentStep === 5) ? '' : 'none';",

    "    var html = '';",
    "    var s = wizardState;",

    // Step 1: Base Profile.
    "    if(s.currentStep === 1) {",
    "      html += '<div class=\"field-group\">';",
    "      html += '<label for=\"wizard-profile-name\">Profile Name</label>';",
    "      var nameReadonly = s.editMode ? ' readonly style=\"opacity: 0.6; cursor: not-allowed;\"' : '';",
    "      html += '<input type=\"text\" id=\"wizard-profile-name\" value=\"' + s.profileName.replace(/\"/g, '&quot;') +",
    "        '\" placeholder=\"e.g., myProvider\"' + nameReadonly + '>';",
    "      html += '<div class=\"field-hint\">' + (s.editMode ? 'Profile name cannot be changed.' :",
    "        'Unique identifier. Letters, numbers, and hyphens only.') + '</div>';",
    "      html += '</div>';",
    "      html += '<label>Base Profile</label>';",
    "      var profiles = window.__wizardProfiles || {};",
    "      var cats = [",
    "        { key: 'api', title: 'Fullscreen API',",
    "          desc: 'For single-channel sites that require JavaScript\\'s requestFullscreen() API instead of keyboard shortcuts.' },",
    "        { key: 'keyboard', title: 'Keyboard Fullscreen',",
    "          desc: 'For single-channel sites that use the \\'f\\' key to toggle fullscreen mode.' },",
    "        { key: 'multiChannel', title: 'Multi-Channel',",
    "          desc: 'For sites that host multiple live channels on a single page.' },",
    "        { key: 'special', title: 'Special',",
    "          desc: 'For non-standard use cases like static pages without video.' },",
    "        { key: 'custom', title: 'Custom', desc: '' }",
    "      ];",
    "      for(var ci = 0; ci < cats.length; ci++) {",
    "        var cat = cats[ci];",
    "        var items = (profiles[cat.key] || []).filter(function(p) { return !s.editMode || p.name !== s.editKey; });",
    "        if(items.length === 0) continue;",
    "        html += '<div class=\"wizard-profile-category\">';",
    "        html += '<h4>' + cat.title + '</h4>';",
    "        if(cat.desc) html += '<div class=\"wizard-category-desc\">' + esc(cat.desc) + '</div>';",
    "        for(var pi = 0; pi < items.length; pi++) {",
    "          var p = items[pi];",
    "          var checked = (s.baseProfile === p.name) ? ' checked' : '';",
    "          html += '<label class=\"wizard-profile-option\">';",
    "          html += '<input type=\"radio\" name=\"baseProfile\" value=\"' + p.name + '\"' + checked + '>';",
    "          var sumTag = p.summary ? ' <span class=\"profile-option-summary\">(' + esc(p.summary) + ')</span>' : '';",
    "          html += '<div><div class=\"profile-option-name\">' + esc(p.name) + sumTag + '</div>';",
    "          html += '<div class=\"profile-option-desc\">' + esc(p.description) + '</div></div>';",
    "          html += '</label>';",
    "        }",
    "        html += '</div>';",
    "      }",
    "    }",

    // Step 2: Channel Selection Strategy. Strategies and their fields are rendered from the server-side registry (window.__wizardStrategies).
    "    if(s.currentStep === 2) {",
    "      html += '<label>Channel Selection Strategy</label>';",
    "      var strategies = window.__wizardStrategies;",
    "      for(var si = 0; si < strategies.length; si++) {",
    "        var st = strategies[si];",
    "        var checked = (s.strategy === st.id) ? ' checked' : '';",
    "        html += '<label class=\"wizard-strategy-option\">';",
    "        html += '<input type=\"radio\" name=\"strategy\" value=\"' + st.id + '\"' + checked + '>';",
    "        html += '<div><div class=\"wizard-strategy-label\">' + st.name + '</div>';",
    "        html += '<div class=\"wizard-strategy-desc\">' + st.description + '</div></div>';",
    "        html += '</label>';",
    "      }",
    "      var currentStrat = strategies.find(function(st) { return st.id === s.strategy; });",
    "      if(currentStrat && currentStrat.fields.length > 0) {",
    "        html += '<hr style=\"border: none; border-top: 1px solid var(--border-default); margin: 16px 0;\">';",
    "        for(var fi = 0; fi < currentStrat.fields.length; fi++) {",
    "          var f = currentStrat.fields[fi];",
    "          if(f.type === 'text') {",
    "            var val = (s.strategyFields[f.id] || '').replace(/\"/g, '&quot;');",
    "            var optLabel = f.required ? '' : ' <span style=\"color: var(--text-muted);\">(optional)</span>';",
    "            html += '<div class=\"field-group\">';",
    "            html += '<label for=\"wizard-sf-' + f.id + '\">' + f.label + optLabel + '</label>';",
    "            html += '<input type=\"text\" id=\"wizard-sf-' + f.id + '\" data-field=\"' + f.id + '\" value=\"' + val + '\"';",
    "            if(f.placeholder) html += ' placeholder=\"' + f.placeholder.replace(/\"/g, '&quot;') + '\"';",
    "            html += '>';",
    "            if(f.hint) html += '<div class=\"field-hint\">' + f.hint + '</div>';",
    "            html += '</div>';",
    "          } else if(f.type === 'boolean') {",
    "            var boolChecked = s.strategyFields[f.id] ? ' checked' : '';",
    "            html += '<label class=\"wizard-flag\">';",
    "            html += '<input type=\"checkbox\" data-field=\"' + f.id + '\"' + boolChecked + '>';",
    "            html += '<div><div class=\"wizard-flag-label\">' + f.label + '</div>';",
    "            if(f.hint) html += '<div class=\"wizard-flag-desc\">' + f.hint + '</div>';",
    "            html += '</div></label>';",
    "          }",
    "        }",
    "      }",
    "    }",

    // Step 3: Override Flags. Flags are rendered from the server-side registry (window.__wizardFlags).
    "    if(s.currentStep === 3) {",
    "      html += '<label>Profile Flags</label>';",
    "      html += '<div class=\"field-hint\" style=\"margin-bottom: 12px;\">These override the base profile. ' +",
    "        'Only change what differs from ' + s.baseProfile + '.</div>';",
    "      var wizFlags = window.__wizardFlags;",
    "      for(var fi = 0; fi < wizFlags.length; fi++) {",
    "        var f = wizFlags[fi];",
    "        var checked = s.flags[f.id] ? ' checked' : '';",
    "        html += '<label class=\"wizard-flag\">';",
    "        html += '<input type=\"checkbox\" name=\"flag-' + f.id + '\"' + checked + '>';",
    "        html += '<div><div class=\"wizard-flag-label\">' + f.label + '</div>';",
    "        html += '<div class=\"wizard-flag-desc\">' + f.description + '</div></div>';",
    "        html += '</label>';",
    "      }",
    "      html += '<div class=\"field-group\" style=\"margin-top: 12px;\">';",
    "      html += '<label for=\"wizard-hide-selector\">Hide Selector (CSS) <span style=\"color: var(--text-muted);\">(optional)</span></label>';",
    "      html += '<input type=\"text\" id=\"wizard-hide-selector\" value=\"' + s.hideSelector.replace(/\"/g, '&quot;') +",
    "        '\" placeholder=\"e.g., .overlay, .ad-banner\">';",
    "      html += '<div class=\"field-hint\">Overlay elements to hide during capture.</div>';",
    "      html += '</div>';",
    "    }",

    // Step 4: Domain Mapping.
    "    if(s.currentStep === 4) {",
    "      html += '<label>Domain Mapping</label>';",
    "      html += '<div class=\"field-hint\" style=\"margin-bottom: 12px;\">Map a streaming site domain to this profile.</div>';",
    "      for(var di = 0; di < s.domains.length; di++) {",
    "        var d = s.domains[di];",
    "        html += '<div class=\"wizard-domain-row\" data-domain-idx=\"' + di + '\">';",
    "        html += '<div class=\"field-group\">';",
    "        html += '<label>Domain</label>';",
    "        html += '<input type=\"text\" class=\"domain-input\" value=\"' + d.domain.replace(/\"/g, '&quot;') + '\" placeholder=\"e.g., watch.sling.com\">';",
    "        html += '</div>';",
    "        html += '<div class=\"field-group\">';",
    "        html += '<label>Provider Name</label>';",
    "        html += '<input type=\"text\" class=\"provider-input\" value=\"' + d.provider.replace(/\"/g, '&quot;') + '\" placeholder=\"e.g., Sling TV\">';",
    "        html += '</div>';",
    "        html += '<div class=\"field-group\">';",
    "        html += '<label>Provider Tag <span style=\"color: var(--text-muted);\">(auto-filled)</span></label>';",
    "        html += '<input type=\"text\" class=\"provider-tag-input\" value=\"' + d.providerTag.replace(/\"/g, '&quot;') +",
    "          '\" placeholder=\"auto-generated\">';",
    "        html += '<div class=\"field-hint\">Identifier for the provider filter. Auto-generated from provider name.</div>';",
    "        html += '</div>';",
    "        html += '</div>';",
    "      }",
    "      html += '<button type=\"button\" class=\"wizard-add-domain\" onclick=\"addWizardDomain()\">+ Add another domain</button>';",
    "    }",

    // Step 5: Review and Save.
    "    if(s.currentStep === 5) {",
    "      html += '<label>Review</label>';",
    "      html += '<table class=\"wizard-review-table\">';",
    "      html += '<tr><td>Profile</td><td>' + esc(s.profileName) + '</td></tr>';",
    "      html += '<tr><td>Extends</td><td>' + esc(s.baseProfile) + '</td></tr>';",
    "      html += '<tr><td>Strategy</td><td>' + esc(s.strategy) + '</td></tr>';",
    "      var domainList = s.domains.filter(function(d) { return d.domain; }).map(function(d) { return esc(d.domain); }).join(', ');",
    "      html += '<tr><td>Domain(s)</td><td>' + (domainList || 'none') + '</td></tr>';",
    "      var providerList = s.domains.filter(function(d) { return d.provider; }).map(function(d) { return esc(d.provider); }).join(', ');",
    "      html += '<tr><td>Provider</td><td>' + (providerList || 'none') + '</td></tr>';",
    "      if(s.strategy !== 'none') {",
    "        var reviewStrat = window.__wizardStrategies.find(function(st) { return st.id === s.strategy; });",
    "        if(reviewStrat) {",
    "          for(var ri = 0; ri < reviewStrat.fields.length; ri++) {",
    "            var rf = reviewStrat.fields[ri];",
    "            var rv = s.strategyFields[rf.id];",
    "            if(rf.type === 'text' && rv) {",
    "              html += '<tr><td>' + esc(rf.label.replace(/ \\(CSS\\)/, '')) + '</td><td><code>' + esc(rv) + '</code></td></tr>';",
    "            } else if(rf.type === 'boolean' && rv) {",
    "              html += '<tr><td>' + esc(rf.label) + '</td><td>Yes</td></tr>';",
    "            }",
    "          }",
    "        }",
    "      }",
    "      var flagNames = Object.keys(s.flags).filter(function(k) { return s.flags[k]; });",
    "      if(flagNames.length > 0) {",
    "        html += '<tr><td>Flags</td><td>' + esc(flagNames.join(', ')) + '</td></tr>';",
    "      }",
    "      if(s.hideSelector) html += '<tr><td>Hide Selector</td><td><code>' + esc(s.hideSelector) + '</code></td></tr>';",
    "      html += '</table>';",
    "      html += '<div class=\"field-group\" style=\"margin-top: 16px;\">';",
    "      html += '<label for=\"wizard-description\">Description <span style=\"color: var(--text-muted);\">(optional)</span></label>';",
    "      html += '<textarea id=\"wizard-description\" placeholder=\"Optional description for documentation.\">' + esc(s.description) + '</textarea>';",
    "      html += '</div>';",
    "    }",

    "    content.innerHTML = html;",

    // Attach live event handlers after rendering.
    "    attachWizardHandlers();",
    "  }",

    // Attach event handlers to dynamically rendered wizard inputs.
    "  function attachWizardHandlers() {",
    "    var s = wizardState;",

    // Step 1: profile name and base profile radio.
    "    var nameInput = document.getElementById('wizard-profile-name');",
    "    if(nameInput) nameInput.addEventListener('input', function() { s.profileName = this.value; });",
    "    var radios = document.querySelectorAll('input[name=\"baseProfile\"]');",
    "    for(var i = 0; i < radios.length; i++) {",
    "      radios[i].addEventListener('change', function() { s.baseProfile = this.value; });",
    "    }",

    // Step 2: strategy radio and strategy field inputs. Text and boolean fields are bound by data-field attribute.
    "    var stratRadios = document.querySelectorAll('input[name=\"strategy\"]');",
    "    for(var i = 0; i < stratRadios.length; i++) {",
    "      stratRadios[i].addEventListener('change', function() { s.strategy = this.value; renderWizardStep(); });",
    "    }",
    "    var sfTextInputs = document.querySelectorAll('#wizard-content input[type=\"text\"][data-field]');",
    "    for(var i = 0; i < sfTextInputs.length; i++) {",
    "      sfTextInputs[i].addEventListener('input', function() { s.strategyFields[this.getAttribute('data-field')] = this.value; });",
    "    }",
    "    var sfCheckInputs = document.querySelectorAll('#wizard-content input[type=\"checkbox\"][data-field]');",
    "    for(var i = 0; i < sfCheckInputs.length; i++) {",
    "      sfCheckInputs[i].addEventListener('change', function() {",
    "        if(this.checked) { s.strategyFields[this.getAttribute('data-field')] = true; }",
    "        else { delete s.strategyFields[this.getAttribute('data-field')]; }",
    "      });",
    "    }",

    // Step 3: flag checkboxes and hide selector.
    "    var flagCbs = document.querySelectorAll('input[name^=\"flag-\"]');",
    "    for(var i = 0; i < flagCbs.length; i++) {",
    "      flagCbs[i].addEventListener('change', function() {",
    "        var flagName = this.name.replace('flag-', '');",
    "        if(this.checked) { s.flags[flagName] = true; } else { delete s.flags[flagName]; }",
    "      });",
    "    }",
    "    var hideInput = document.getElementById('wizard-hide-selector');",
    "    if(hideInput) hideInput.addEventListener('input', function() { s.hideSelector = this.value; });",

    // Step 4: domain inputs with auto-fill for provider tag.
    "    var domainRows = document.querySelectorAll('.wizard-domain-row');",
    "    for(var i = 0; i < domainRows.length; i++) {",
    "      (function(idx) {",
    "        var row = domainRows[idx];",
    "        var domInput = row.querySelector('.domain-input');",
    "        var provInput = row.querySelector('.provider-input');",
    "        var tagInput = row.querySelector('.provider-tag-input');",
    "        if(domInput) domInput.addEventListener('input', function() { s.domains[idx].domain = this.value; });",
    "        if(provInput) provInput.addEventListener('input', function() {",
    "          s.domains[idx].provider = this.value;",
    "          var tag = autoProviderTag(this.value);",
    "          s.domains[idx].providerTag = tag;",
    "          if(tagInput) tagInput.value = tag;",
    "        });",
    "        if(tagInput) tagInput.addEventListener('input', function() { s.domains[idx].providerTag = this.value; });",
    "      })(i);",
    "    }",

    // Step 5: description textarea.
    "    var descInput = document.getElementById('wizard-description');",
    "    if(descInput) descInput.addEventListener('input', function() { s.description = this.value; });",
    "  }",

    // Open the wizard modal and reset state.
    "  window.openWizard = function() {",
    "    wizardState = {",
    "      currentStep: 1, editKey: null, editMode: false, highestStep: 1, profileName: '', baseProfile: '', strategy: 'none',",
    "      strategyFields: {}, flags: {}, hideSelector: '',",
    "      domains: [{ domain: '', provider: '', providerTag: '' }], description: ''",
    "    };",
    "    var title = document.getElementById('wizard-title');",
    "    if(title) title.textContent = 'New Provider Profile';",
    "    document.getElementById('wizard-modal').style.display = 'flex';",
    "    renderWizardStep();",
    "  };",

    // Close the wizard modal.
    "  window.closeWizard = function() {",
    "    document.getElementById('wizard-modal').style.display = 'none';",
    "  };",

    // Add another domain row.
    "  window.addWizardDomain = function() {",
    "    wizardState.domains.push({ domain: '', provider: '', providerTag: '' });",
    "    renderWizardStep();",
    "  };",

    // Validate current step. Returns error message or empty string.
    "  function validateStep() {",
    "    var s = wizardState;",
    "    if(s.currentStep === 1) {",
    "      if(!s.editMode) {",
    "        if(!s.profileName.trim()) return 'Profile name is required.';",
    "        if(!/^[a-zA-Z]([a-zA-Z0-9-]*[a-zA-Z0-9])?$/.test(s.profileName))",
    "          return 'Must start with a letter, contain only letters, numbers, and hyphens, and not end with a hyphen.';",
    "        if(s.profileName.length > 50) return 'Profile name must be 50 characters or less.';",
    "      }",
    "      if(!s.baseProfile) return 'Select a base profile.';",
    "    }",
    "    if(s.currentStep === 2) {",
    "      var valStrat = window.__wizardStrategies.find(function(st) { return st.id === s.strategy; });",
    "      if(valStrat) {",
    "        for(var vi = 0; vi < valStrat.fields.length; vi++) {",
    "          var vf = valStrat.fields[vi];",
    "          if(vf.required && (vf.type === 'text') && !(s.strategyFields[vf.id] || '').trim()) {",
    "            return vf.label.replace(/ \\(CSS\\)/, '') + ' is required for the ' + valStrat.name + ' strategy.';",
    "          }",
    "        }",
    "      }",
    "    }",
    "    if(s.currentStep === 4) {",
    "      var hasDomain = s.domains.some(function(d) { return d.domain.trim(); });",
    "      if(!hasDomain) return 'At least one domain is required.';",
    "    }",
    "    return '';",
    "  }",

    // Navigate to next step.
    "  window.wizardNext = function() {",
    "    var err = validateStep();",
    "    var errDiv = document.getElementById('wizard-error');",
    "    if(err) {",
    "      errDiv.textContent = err;",
    "      errDiv.style.display = 'block';",
    "      return;",
    "    }",
    "    if(wizardState.currentStep < 5) {",
    "      wizardState.currentStep++;",
    "      if(wizardState.currentStep > wizardState.highestStep) wizardState.highestStep = wizardState.currentStep;",
    "      renderWizardStep();",
    "    }",
    "  };",

    // Navigate to previous step.
    "  window.wizardBack = function() {",
    "    if(wizardState.currentStep > 1) {",
    "      wizardState.currentStep--;",
    "      renderWizardStep();",
    "    }",
    "  };",

    // Navigate to a specific step by clicking the step indicator. Going backward is always allowed for visited steps. Going forward requires validation of the
    // current step and can only reach steps that have been visited before (up to highestStep).
    "  function wizardGoToStep(targetStep) {",
    "    if(targetStep === wizardState.currentStep) return;",
    "    if(targetStep > wizardState.highestStep) return;",
    "    if(targetStep > wizardState.currentStep) {",
    "      var err = validateStep();",
    "      if(err) {",
    "        var errDiv = document.getElementById('wizard-error');",
    "        errDiv.textContent = err;",
    "        errDiv.style.display = 'block';",
    "        return;",
    "      }",
    "    }",
    "    wizardState.currentStep = targetStep;",
    "    renderWizardStep();",
    "  }",

    // Save the profile to the server.
    "  window.saveProfile = function(andTest) {",
    "    var s = wizardState;",
    "    var profile = { extends: s.baseProfile };",
    "    if(s.strategy !== 'none') {",
    "      var cs = { strategy: s.strategy };",
    "      var saveStrat = window.__wizardStrategies.find(function(st) { return st.id === s.strategy; });",
    "      if(saveStrat) {",
    "        for(var si = 0; si < saveStrat.fields.length; si++) {",
    "          var sf = saveStrat.fields[si];",
    "          var sfVal = s.strategyFields[sf.id];",
    "          if(sf.type === 'text' && sfVal) { cs[sf.id] = sfVal; }",
    "          else if(sf.type === 'boolean' && sfVal) { cs[sf.id] = true; }",
    "        }",
    "      }",
    "      profile.channelSelection = cs;",
    "    }",
    "    var flagKeys = Object.keys(s.flags).filter(function(k) { return s.flags[k]; });",
    "    for(var i = 0; i < flagKeys.length; i++) { profile[flagKeys[i]] = true; }",
    "    if(s.hideSelector) profile.hideSelector = s.hideSelector;",
    "    if(s.description) profile.description = s.description;",
    "    var domains = {};",
    "    for(var i = 0; i < s.domains.length; i++) {",
    "      var d = s.domains[i];",
    "      if(d.domain.trim()) {",
    "        domains[d.domain.trim()] = { profile: s.profileName };",
    "        if(d.provider.trim()) domains[d.domain.trim()].provider = d.provider.trim();",
    "        if(d.providerTag.trim()) domains[d.domain.trim()].providerTag = d.providerTag.trim();",
    "      }",
    "    }",
    "    var body = { domains: domains, key: s.profileName, profile: profile };",
    "    fetch('/config/profiles', {",
    "      method: 'POST',",
    "      headers: { 'Content-Type': 'application/json' },",
    "      body: JSON.stringify(body)",
    "    })",
    "      .then(function(r) { return r.json(); })",
    "      .then(function(data) {",
    "        if(data.success) {",
    "          closeWizard();",
    "          if(andTest && s.domains[0] && s.domains[0].domain) {",
    "            startProfileTest(s.domains[0].domain);",
    "          } else {",
    "            location.reload();",
    "          }",
    "        } else {",
    "          var errDiv = document.getElementById('wizard-error');",
    "          errDiv.textContent = data.error || 'Failed to save profile.';",
    "          errDiv.style.display = 'block';",
    "        }",
    "      })",
    "      .catch(function() {",
    "        var errDiv = document.getElementById('wizard-error');",
    "        errDiv.textContent = 'Request failed.';",
    "        errDiv.style.display = 'block';",
    "      });",
    "  };",

    // Test flow: open a browser to a URL with the profile applied and allow selector checking.
    "  var testSelectors = {};",

    // Start a profile test by opening the browser to a URL.
    "  window.startProfileTest = function(url) {",
    "    var s = wizardState;",
    "    testSelectors = {};",
    "    if(s.strategy !== 'none' && s.strategyFields.matchSelector) testSelectors.matchSelector = s.strategyFields.matchSelector;",
    "    if(s.strategyFields.playSelector) testSelectors.playSelector = s.strategyFields.playSelector;",
    "    if(s.hideSelector) testSelectors.hideSelector = s.hideSelector;",
    "    var testUrl = url.indexOf('://') === -1 ? 'https://' + url : url;",
    "    fetch('/config/profiles/test', {",
    "      method: 'POST',",
    "      headers: { 'Content-Type': 'application/json' },",
    "      body: JSON.stringify({ url: testUrl })",
    "    })",
    "      .then(function(r) { return r.json(); })",
    "      .then(function(data) {",
    "        if(data.success) {",
    "          document.getElementById('test-modal').style.display = 'flex';",
    "          document.getElementById('test-selector-results').style.display = 'none';",
    "          document.getElementById('test-check-btn').style.display = ",
    "            Object.keys(testSelectors).length > 0 ? '' : 'none';",
    "        } else {",
    "          showToast('Failed to start test: ' + (data.error || 'Unknown error.'), 'error');",
    "        }",
    "      })",
    "      .catch(function() { showToast('Failed to start test.', 'error'); });",
    "  };",

    // Check CSS selectors against the live page.
    "  window.checkSelectors = function() {",
    "    if(Object.keys(testSelectors).length === 0) return;",
    "    fetch('/config/profiles/test/check', {",
    "      method: 'POST',",
    "      headers: { 'Content-Type': 'application/json' },",
    "      body: JSON.stringify({ selectors: testSelectors })",
    "    })",
    "      .then(function(r) { return r.json(); })",
    "      .then(function(data) {",
    "        var resultsDiv = document.getElementById('test-selector-results');",
    "        if(!data.success || !data.results) {",
    "          resultsDiv.innerHTML = '<p style=\"color: var(--status-error-text);\">Check failed.</p>';",
    "          resultsDiv.style.display = 'block';",
    "          return;",
    "        }",
    "        var html = '<table style=\"width: 100%; font-size: 13px; margin: 12px 0;\">';",
    "        var names = Object.keys(data.results);",
    "        for(var i = 0; i < names.length; i++) {",
    "          var name = names[i];",
    "          var r = data.results[name];",
    "          var color = r.valid && r.count > 0 ? 'var(--status-success-text)' : 'var(--status-error-text)';",
    "          var text = !r.valid ? 'Invalid selector' : r.count + ' element(s)';",
    "          html += '<tr><td style=\"padding: 4px 8px;\">' + esc(name) + '</td>';",
    "          html += '<td style=\"padding: 4px 8px; color: ' + color + ';\">' + text + '</td></tr>';",
    "        }",
    "        html += '</table>';",
    "        resultsDiv.innerHTML = html;",
    "        resultsDiv.style.display = 'block';",
    "      })",
    "      .catch(function() {",
    "        var resultsDiv = document.getElementById('test-selector-results');",
    "        resultsDiv.innerHTML = '<p style=\"color: var(--status-error-text);\">Check request failed.</p>';",
    "        resultsDiv.style.display = 'block';",
    "      });",
    "  };",

    // End the test flow and close the test dialog.
    "  window.endProfileTest = function() {",
    "    fetch('/config/profiles/test/done', { method: 'POST' })",
    "      .then(function() {",
    "        document.getElementById('test-modal').style.display = 'none';",
    "        location.reload();",
    "      })",
    "      .catch(function() {",
    "        document.getElementById('test-modal').style.display = 'none';",
    "      });",
    "  };",

    // Initialize channels subtab on load: hash > localStorage > default.
    "  var initialSubtab = window.initialChannelsHashSubtab;",
    "  if(!initialSubtab) {",
    "    try { initialSubtab = localStorage.getItem('prismcast-channels-subtab'); } catch(e) {}",
    "  }",
    "  if(initialSubtab && document.querySelector('.channels-subtab-btn[data-channels-subtab=\"' + initialSubtab + '\"]')) {",
    "    switchChannelsSubtab(initialSubtab, false);",
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
    "#stream-count { background: none; border: none; color: inherit; font: inherit; padding: 0; }",
    "#stream-count.clickable { cursor: pointer; }",
    "#stream-count.clickable:hover { color: var(--text-primary); }",
    ".stream-popover .dropdown-menu { right: 0; left: auto; min-width: 220px; }",
    ".stream-popover-row { display: flex; align-items: center; gap: 8px; padding: 6px 12px; font-size: 13px; white-space: nowrap; }",
    ".stream-popover-logo { height: 18px; width: auto; max-width: 80px; vertical-align: middle; }",
    ".stream-popover-channel { color: var(--text-primary); }",
    ".stream-popover-show { color: var(--text-muted); }",
    ".stream-popover-duration { color: var(--text-muted); margin-left: auto; }",

    // Subtab styles for Configuration tab.
    ".subtab-bar { display: flex; border-bottom: 1px solid var(--border-default); margin-bottom: 20px; gap: 2px; flex-wrap: wrap; }",
    ".subtab-btn { padding: 8px 16px; border: none; background: var(--subtab-bg); cursor: pointer; font-size: 13px; font-weight: 500; ",
    "color: var(--tab-text); border-radius: var(--radius-md) var(--radius-md) 0 0; transition: all 0.2s; }",
    ".subtab-btn:hover { background: var(--subtab-bg-hover); color: var(--tab-text-hover); }",
    ".subtab-btn.active { background: var(--subtab-bg-active); color: var(--subtab-text-active); border-bottom: 2px solid var(--subtab-border-active); }",
    ".subtab-panel { display: none; }",
    ".subtab-panel.active { display: block; }",

    // Channels tab subtab styles. Scoped to avoid collision with Configuration tab subtabs.
    ".channels-subtab-bar { display: flex; border-bottom: 1px solid var(--border-default); margin-bottom: 20px; gap: 2px; flex-wrap: wrap; }",
    ".channels-subtab-btn { padding: 8px 16px; border: none; background: var(--subtab-bg); cursor: pointer; font-size: 13px; font-weight: 500; ",
    "color: var(--tab-text); border-radius: var(--radius-md) var(--radius-md) 0 0; transition: all 0.2s; }",
    ".channels-subtab-btn:hover { background: var(--subtab-bg-hover); color: var(--tab-text-hover); }",
    ".channels-subtab-btn.active { background: var(--subtab-bg-active); color: var(--subtab-text-active); ",
    "border-bottom: 2px solid var(--subtab-border-active); }",
    ".channels-subtab-panel { display: none; }",
    ".channels-subtab-panel.active { display: block; }",

    // Empty state styling for Providers panel.
    ".empty-state { text-align: center; padding: 40px 20px; color: var(--text-secondary); }",
    ".empty-state-title { font-size: 16px; font-weight: 600; color: var(--text-primary); margin: 0 0 8px 0; }",
    ".empty-state-text { font-size: 14px; margin: 0; max-width: 500px; margin-left: auto; margin-right: auto; line-height: 1.5; }",

    // Wizard modal styles.
    ".wizard-modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 1000; ",
    "justify-content: center; align-items: center; }",
    ".wizard-modal-content { background: var(--surface-overlay); border-radius: var(--radius-lg); box-shadow: 0 8px 32px rgba(0,0,0,0.3); ",
    "width: 640px; max-width: 90vw; max-height: 85vh; display: flex; flex-direction: column; overflow: hidden; }",
    ".wizard-header { display: flex; justify-content: space-between; align-items: center; padding: 16px 20px; ",
    "border-bottom: 1px solid var(--border-default); }",
    ".wizard-header h3 { margin: 0; font-size: 18px; color: var(--text-heading); }",
    ".wizard-close { background: none; border: none; font-size: 20px; cursor: pointer; color: var(--text-muted); padding: 4px 8px; }",
    ".wizard-close:hover { color: var(--text-primary); }",

    // Step indicator.
    ".wizard-steps { display: flex; align-items: center; justify-content: center; padding: 16px 20px; gap: 4px; }",
    ".wizard-step { display: flex; flex-direction: column; align-items: center; gap: 4px; }",
    ".wizard-step-line { width: 32px; height: 2px; background: var(--border-default); margin-bottom: 18px; }",
    ".step-circle { width: 28px; height: 28px; border-radius: 50%; display: flex; align-items: center; justify-content: center; ",
    "font-size: 13px; font-weight: 600; border: 2px solid var(--border-default); color: var(--text-muted); background: var(--surface-overlay); }",
    ".wizard-step.active .step-circle { border-color: var(--interactive-primary); color: var(--interactive-primary); background: var(--surface-overlay); }",
    ".wizard-step.completed .step-circle { border-color: var(--interactive-primary); color: #fff; background: var(--interactive-primary); }",
    ".step-label { font-size: 11px; color: var(--text-muted); }",
    ".wizard-step.active .step-label { color: var(--interactive-primary); font-weight: 600; }",
    ".wizard-step.clickable { cursor: pointer; }",
    ".wizard-step.clickable:not(.completed) .step-circle:hover { border-color: var(--interactive-primary); color: var(--interactive-primary); }",

    // Wizard content area.
    ".wizard-content { padding: 20px; overflow-y: auto; flex: 1; min-height: 300px; }",
    ".wizard-content label { display: block; margin-bottom: 6px; font-weight: 500; color: var(--text-primary); font-size: 14px; }",
    ".wizard-content input[type=\"text\"], .wizard-content textarea { width: 100%; padding: 8px 10px; border: 1px solid var(--border-default); ",
    "border-radius: var(--radius-md); font-size: 14px; font-family: inherit; background: var(--surface-overlay); color: var(--text-primary); ",
    "box-sizing: border-box; }",
    ".wizard-content input[type=\"text\"]:focus, .wizard-content textarea:focus { outline: none; border-color: var(--interactive-primary); }",
    ".wizard-content textarea { resize: vertical; min-height: 60px; }",
    ".wizard-content .field-hint { font-size: 12px; color: var(--text-muted); margin-top: 4px; margin-bottom: 12px; }",
    ".wizard-content .field-group { margin-bottom: 16px; }",

    // Wizard profile radio buttons.
    ".wizard-profile-category { margin-bottom: 16px; }",
    ".wizard-profile-category h4 { font-size: 13px; color: var(--text-secondary); margin: 0 0 4px 0; text-transform: uppercase; letter-spacing: 0.5px; }",
    ".wizard-category-desc { font-size: 12px; color: var(--text-muted); margin-bottom: 8px; }",
    ".wizard-content label.wizard-profile-option { display: flex; align-items: flex-start; gap: 8px; padding: 8px 10px; ",
    "border: 1px solid var(--border-default); border-radius: var(--radius-md); margin-bottom: 4px; cursor: pointer; }",
    ".wizard-profile-option:hover { background: var(--surface-hover); }",
    ".wizard-profile-option input[type=\"radio\"] { margin-top: 5px; flex-shrink: 0; }",
    ".wizard-profile-option .profile-option-name { font-weight: 500; font-size: 14px; color: var(--text-primary); }",
    ".wizard-profile-option .profile-option-summary { font-weight: 400; font-size: 12px; color: var(--text-muted); }",
    ".wizard-profile-option .profile-option-desc { font-size: 12px; color: var(--text-secondary); margin-top: 2px; }",

    // Wizard strategy radio buttons.
    ".wizard-content label.wizard-strategy-option { display: flex; align-items: flex-start; gap: 8px; padding: 10px 12px; ",
    "border: 1px solid var(--border-default); border-radius: var(--radius-md); margin-bottom: 6px; cursor: pointer; }",
    ".wizard-strategy-option:hover { background: var(--surface-hover); }",
    ".wizard-strategy-option input[type=\"radio\"] { margin-top: 5px; flex-shrink: 0; }",
    ".wizard-strategy-label { font-weight: 500; font-size: 14px; color: var(--text-primary); }",
    ".wizard-strategy-desc { font-size: 12px; color: var(--text-secondary); margin-top: 2px; }",

    // Wizard flag checkboxes.
    ".wizard-content label.wizard-flag { display: flex; align-items: flex-start; gap: 8px; padding: 8px 10px; margin-bottom: 4px; }",
    ".wizard-flag input[type=\"checkbox\"] { margin-top: 5px; flex-shrink: 0; }",
    ".wizard-flag-label { font-size: 14px; color: var(--text-primary); }",
    ".wizard-flag-desc { font-size: 12px; color: var(--text-secondary); margin-top: 2px; }",

    // Wizard review summary.
    ".wizard-review-table { width: 100%; border-collapse: collapse; font-size: 14px; }",
    ".wizard-review-table td { padding: 6px 8px; border-bottom: 1px solid var(--border-default); }",
    ".wizard-review-table td:first-child { font-weight: 500; color: var(--text-secondary); width: 120px; white-space: nowrap; }",

    // Wizard validation error.
    ".wizard-error { padding: 0 20px; color: var(--status-error-text); font-size: 13px; }",

    // Wizard buttons.
    ".wizard-buttons { display: flex; justify-content: space-between; align-items: center; padding: 16px 20px; ",
    "border-top: 1px solid var(--border-default); }",
    ".wizard-buttons-right { display: flex; gap: 8px; }",

    // Domain row for adding multiple domains.
    ".wizard-domain-row { border: 1px solid var(--border-default); border-radius: var(--radius-md); padding: 12px; margin-bottom: 8px; }",
    ".wizard-domain-row .field-group:last-child { margin-bottom: 0; }",
    ".wizard-add-domain { background: none; border: none; color: var(--interactive-primary); cursor: pointer; font-size: 13px; padding: 4px 0; }",
    ".wizard-add-domain:hover { text-decoration: underline; }",

    // Export and import modal styles: body area, profile list items, section header, and option labels.
    ".export-modal-body { padding: 20px; overflow-y: auto; font-size: 14px; }",
    ".export-section-header { margin-bottom: 12px; font-weight: 500; }",
    ".export-option-label { display: flex; align-items: center; gap: 8px; cursor: pointer; }",
    ".export-profile-item { display: flex; align-items: center; gap: 10px; padding: 8px 10px; margin-bottom: 4px; ",
    "border: 1px solid var(--border-default); border-radius: var(--radius-md); cursor: pointer; }",
    ".export-profile-item:hover { background: var(--surface-hover); }",
    ".export-profile-info { min-width: 0; }",
    ".export-profile-name { font-weight: 600; font-size: 14px; }",
    ".export-profile-meta { font-size: 12px; color: var(--text-muted); }",
    ".export-divider { border-top: 1px solid var(--border-default); margin: 16px 0; }",
    ".export-hint { font-size: 12px; color: var(--text-muted); margin-top: 6px; margin-left: 24px; }",
    ".import-summary-table { width: 100%; font-size: 13px; margin: 12px 0; }",
    ".import-summary-table td { padding: 4px 0; }",
    ".import-summary-label { color: var(--text-muted); }",

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

    // Channel table styles. The wrapper uses fit-content so it shrinks when columns are hidden, with max-width: 100% to prevent viewport overflow. We use
    // border-collapse: separate so that border-radius works on the header cells. Overflow is auto so the table scrolls on narrow screens; dropdown menus escape
    // the container via portal to <body>.
    ".channel-table-wrapper { width: fit-content; max-width: 100%; margin: 0 auto 20px; border: 1px solid var(--border-default); " +
      "border-radius: var(--radius-lg); overflow: auto; }",
    ".channel-table { width: 100%; border-collapse: separate; border-spacing: 0; table-layout: auto; min-width: 650px; margin: 0; }",
    ".channel-table th, .channel-table td { padding: 10px 12px; text-align: left; border: none; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }",
    ".channel-table th { background: var(--table-header-bg); font-weight: 600; font-size: 13px; border-bottom: 1px solid var(--border-default); }",
    ".channel-table tbody tr:nth-child(even):not(.user-channel) { background: var(--table-row-even); }",
    ".channel-table tr:hover { background: var(--table-row-hover); }",
    ".channel-table .col-key { min-width: 170px; }",
    ".channel-table .col-name { min-width: 200px; }",
    ".channel-table .col-provider { min-width: 200px; }",
    ".channel-table .col-actions, .channel-table td:last-child { min-width: 168px; white-space: nowrap; overflow: visible; }",
    ".provider-select { width: 100%; padding: 2px 4px; font-size: 12px; border: 1px solid var(--form-input-border); ",
    "border-radius: 3px; background: var(--form-input-bg); color: var(--text-primary); }",

    // Optional column width, alignment, and visibility rules. Generated from OPTIONAL_COLUMNS to keep a single source of truth for column metadata.
    ...OPTIONAL_COLUMNS.flatMap((col) => [
      ".channel-table ." + col.cssClass + " { min-width: " + col.width + (col.align === "center" ? "; text-align: center" : "") + "; }",
      ".channel-table.hide-" + col.cssClass + " ." + col.cssClass + " { display: none; }"
    ]),

    // Sortable header styles. Clickable headers with pointer cursor and subtle hover effect.
    ".channel-table th.sortable { cursor: pointer; user-select: none; }",
    ".channel-table th.sortable:hover { background: var(--surface-hover); }",

    // Column picker dropdown. The Actions header uses flex layout to push the ellipsis button to the far right.
    ".channel-table th.col-actions { display: flex; align-items: center; justify-content: space-between; }",
    ".column-picker { position: relative; }",
    ".btn-col-picker { font-size: 16px; line-height: 1; vertical-align: middle; padding: 0 4px; color: var(--text-muted); cursor: pointer; " +
      "background: none; border: none; border-radius: 3px; }",
    ".btn-col-picker:hover { color: var(--text-primary); background: var(--surface-hover); }",
    ".column-picker-menu { min-width: 140px; }",

    // Key column styling: monospace at a slightly smaller size with secondary color to reduce visual weight.
    ".ch-key { color: var(--text-secondary); font-family: var(--font-mono); font-size: 13px; }",

    // Responsive: hide Key and all optional columns on phones.
    "@media (max-width: 768px) { .channel-table .col-key, .channel-table td:nth-child(1), .channel-table th:nth-child(1) { display: none; } " +
      OPTIONAL_COLUMNS.map((col) => ".channel-table ." + col.cssClass).join(", ") + " { display: none; } }",

    // User channel row tinting to distinguish custom/override channels from predefined.
    ".channel-table tr.user-channel { background: var(--user-channel-tint); }",
    ".channel-table tr.user-channel:hover { background: var(--user-channel-tint-hover); }",

    // Disabled predefined channel row styling and hide-disabled toggle.
    ".channel-table tr.channel-disabled { opacity: 0.5; }",
    ".channel-table tr.channel-disabled td { color: var(--text-tertiary); }",
    ".channel-table.hide-disabled tr.channel-disabled { display: none; }",

    // Provider-filtered channel row styling. Uses reduced opacity and italic text to distinguish from manually disabled rows. The compound selector ensures that rows
    // which are both disabled and provider-filtered render at the disabled-level opacity (0.5) rather than the more aggressive unavailable-level opacity (0.4).
    ".channel-table tr.channel-unavailable { opacity: 0.4; font-style: italic; }",
    ".channel-table tr.channel-unavailable td { color: var(--text-tertiary); }",
    ".channel-table tr.channel-unavailable.channel-disabled { opacity: 0.5; }",
    ".channel-table.hide-disabled tr.channel-unavailable { display: none; }",
    ".no-provider-label { color: var(--text-tertiary); font-size: 12px; }",
    ".text-muted { color: var(--text-muted); }",

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
    "background: var(--surface-page); color: var(--text-primary); cursor: pointer; }",

    // Responsive: stack provider toolbar groups vertically on small screens.
    "@media (max-width: 768px) { .provider-toolbar { flex-direction: column; align-items: flex-start; } }",

    // Icon button styling for channel action buttons.
    ".btn-icon { display: inline-flex; align-items: center; justify-content: center; width: 28px; height: 28px; padding: 0; border: none; ",
    "border-radius: var(--radius-md); background: transparent; cursor: pointer; color: var(--text-secondary); transition: color 0.15s, background 0.15s; }",
    ".btn-icon:hover { background: var(--surface-hover); }",
    ".user-channel .btn-icon:hover { background: var(--user-channel-tint-hover); }",
    ".btn-icon-edit:hover { color: var(--interactive-edit); }",
    ".btn-icon-delete:hover { color: var(--interactive-delete); }",
    ".btn-icon-revert:hover { color: var(--interactive-edit); }",
    ".btn-icon-enable:hover { color: var(--interactive-success); }",
    ".btn-icon-disable:hover { color: var(--interactive-delete); }",
    ".btn-icon-login:hover { color: var(--interactive-primary); }",
    ".btn-icon-copy:hover { color: var(--interactive-primary); }",
    ".btn-icon-placeholder { display: inline-block; width: 28px; height: 28px; }",

    // Health indicator colors. The login icon only uses health-success (provider verified). The health icon uses both (channel last-tune result).
    ".health-success { color: var(--interactive-success); }",
    ".health-failed { color: var(--interactive-delete); }",

    // Health icon is non-interactive — no hover effect, default cursor. The .user-channel override needs matching specificity (0-3-0) to prevent the
    // tinted hover background from making the health icon look clickable on user-defined channel rows.
    ".btn-icon-health { cursor: default; }",
    ".btn-icon-health:hover { background: transparent; color: var(--text-secondary); }",
    ".user-channel .btn-icon-health:hover { background: transparent; }",

    // Override hover to preserve health color when set.
    ".btn-icon-health.health-success:hover { color: var(--interactive-success); }",
    ".btn-icon-health.health-failed:hover { color: var(--interactive-delete); }",
    ".copy-dropdown .dropdown-item { font-size: 12px; }",

    // JS tooltip styling. The tooltip element is appended to <body> and positioned via getBoundingClientRect() so it's immune to overflow and stacking contexts.
    // Only activated when the primary input can't hover (hover: none), targeting iPadOS where Safari doesn't show native title tooltips. On pure-touch
    // devices without a trackpad, the JS loads but mouseenter never fires so the tooltip stays hidden. Desktop (hover: hover) skips initialization entirely.
    ".btn-icon-tooltip { position: fixed; padding: 4px 8px; border-radius: var(--radius-sm); background: var(--surface-overlay); color: var(--text-primary); ",
    "font-size: 12px; white-space: nowrap; pointer-events: none; opacity: 0; transition: opacity 0.5s; z-index: 10000; ",
    "box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15); }",
    ".btn-icon-tooltip.visible { opacity: 1; transition: opacity 0.1s; }",

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
    ".profile-reference-close { color: var(--text-secondary); font-size: 18px; background: none; border: none; cursor: pointer; padding: 0 5px; }",
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

    // API Reference index.
    ".api-index { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 12px 24px; margin-top: 12px; }",
    ".api-index-group { display: flex; flex-direction: column; gap: 2px; }",
    ".api-index a { color: var(--text-secondary); text-decoration: none; font-size: 12px; line-height: 1.5; }",
    ".api-index a:hover { color: var(--interactive-primary); }",
    ".api-index a code { font-size: 11px; }",
    ".api-index-heading { font-weight: 600; font-size: 13px !important; color: var(--text-primary) !important; margin-bottom: 1px; }",
    ".api-index-desc { color: var(--text-muted); font-size: 11px; margin-bottom: 3px; }",

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
