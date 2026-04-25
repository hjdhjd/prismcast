/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * status.ts: Client-side JavaScript generator for the PrismCast status display.
 */

export function generateStatusScript(): string {

  return [
    "<script>",
    "let statusEventSource = null;",
    "let streamData = {};",
    "let systemData = null;",
    "const expandedStreams = {};",
    "const healthColorVars = { healthy: 'var(--stream-healthy)', buffering: 'var(--stream-buffering)', recovering: 'var(--stream-recovering)', ",
    "  stalled: 'var(--stream-stalled)', error: 'var(--stream-error)' };",

    // Client type labels for the detail row. Hoisted to module scope so formatClients does not reallocate the map on every render.
    "const clientTypeLabels = { hls: 'HLS', mpegts: 'MPEG-TS' };",

    // Resolution labels for the native HLS quality suffix. Hoisted to module scope so renderStreamsTable does not reallocate the map on every stream row.
    "const nativeResolutionLabels = { '360': '360p', '480': '480p', '720': '720p', '1080': '1080p', '2160': '4K' };",

    // Row background tints keyed by health state. Module-scope constant so renderStreamsTable reads from a single shared map instead of allocating a fresh
    // object for every stream row. Consistent with clientTypeLabels and nativeResolutionLabels above - accessed inline at the call site, no wrapper helper.
    "const rowTints = { healthy: 'transparent', buffering: 'var(--stream-tint-buffering)', stalled: 'var(--stream-tint-stalled)', " +
    "recovering: 'var(--stream-tint-recovering)', error: 'var(--stream-tint-error)' };",

    // Format duration in human readable format.
    "function formatDuration(seconds) {",
    "  if(seconds < 60) return seconds + 's';",
    "  if(seconds < 3600) return Math.floor(seconds / 60) + 'm ' + (seconds % 60) + 's';",
    "  const h = Math.floor(seconds / 3600);",
    "  const m = Math.floor((seconds % 3600) / 60);",
    "  return h + 'h ' + m + 'm';",
    "}",

    // Format bytes in human readable format.
    "function formatBytes(bytes) {",
    "  if(bytes === 0) return '0 B';",
    "  if(bytes < 1024) return bytes + ' B';",
    "  if(bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';",
    "  return (bytes / 1048576).toFixed(1) + ' MB';",
    "}",

    // Format absolute time (e.g., "6:54 AM" or "Jan 14, 6:54 AM" if different day).
    "function formatTime(isoString) {",
    "  const date = new Date(isoString);",
    "  const now = new Date();",
    "  let hours = date.getHours();",
    "  const minutes = date.getMinutes();",
    "  const ampm = hours >= 12 ? 'PM' : 'AM';",
    "  hours = hours % 12;",
    "  hours = hours ? hours : 12;",
    "  let timeStr = hours + ':' + (minutes < 10 ? '0' : '') + minutes + ' ' + ampm;",
    "  if(date.toDateString() !== now.toDateString()) {",
    "    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];",
    "    timeStr = months[date.getMonth()] + ' ' + date.getDate() + ', ' + timeStr;",
    "  }",
    "  return timeStr;",
    "}",

    // Extract concise domain from URL for display (last two hostname parts). Mirrors the server-side extractDomain() in utils/format.ts.
    "function getDomain(url) {",
    "  const parsed = URL.parse(url);",
    "  if(!parsed) return url;",
    "  const parts = parsed.hostname.split('.');",
    "  return (parts.length > 2) ? parts.slice(-2).join('.') : parts.join('.');",
    "}",

    // Resolve the recovery-level label for a stream in the recovering state. Kept as a dedicated helper because the mapping is level-based, not string-based,
    // so it cannot fold into the same lookup map as the top-level health labels. Escalation level semantics defined in monitor.ts:
    // L1=play/unmute, L2=seek, L3=source reload, L4=page navigation.
    "function getRecoveringLabel(level) {",
    "  switch(level) {",
    "    case 1: return 'Resuming playback';",
    "    case 2: return 'Syncing to live';",
    "    case 3: return 'Reloading player';",
    "    default: return (level >= 4) ? 'Reloading page' : 'Recovering';",
    "  }",
    "}",

    // Static labels for each top-level health state. The recovering state delegates to getRecoveringLabel which dispatches on the escalation level.
    "const healthLabels = { healthy: 'Healthy', buffering: 'Buffering', stalled: 'Stalled', error: 'Error' };",

    // Get health badge HTML using CSS variables for theme-aware colors.
    "function getHealthBadge(health, level) {",
    "  const label = (health === 'recovering') ? getRecoveringLabel(level) : (healthLabels[health] ?? health);",
    "  return '<span class=\"status-dot\" style=\"color: ' + (healthColorVars[health] ?? 'var(--text-muted)') + ';\">&#9679;</span> ' +",
    "    '<span style=\"color: var(--text-secondary);\">' + label + '</span>';",
    "}",

    // Update system status display in header. Shows system health (green dot when connected, red with label when not) and stream count.
    "function updateSystemStatus() {",
    "  if(!systemData) return;",
    "  const healthEl = document.getElementById('system-health');",
    "  const streamEl = document.getElementById('stream-count');",
    "  if(systemData.browser.connected) {",
    "    healthEl.innerHTML = '<span class=\"status-dot\" style=\"color: var(--stream-healthy);\">&#9679;</span>';",
    "  } else {",
    "    healthEl.innerHTML = '<span class=\"status-dot\" style=\"color: var(--stream-error);\">&#9679;</span> Browser offline';",
    "  }",
    "  const active = systemData.streams.active;",
    "  const limit = systemData.streams.limit;",
    "  if(active === 0) {",
    "    streamEl.textContent = '0 streams';",
    "    streamEl.classList.remove('clickable');",
    "    const popMenu = document.getElementById('stream-popover-menu');",
    "    if(popMenu) popMenu.classList.remove('show');",
    "  } else {",
    "    streamEl.textContent = active + '/' + limit + ' streams';",
    "    streamEl.classList.add('clickable');",
    "  }",
    "}",

    // Build the popover content from streamData. Populates the given menu element with one row per active stream.
    "function buildStreamPopoverContent(menu) {",
    "  let html = '';",
    "  const now = Date.now();",
    "  for(const s of Object.values(streamData)) {",
    "    const color = healthColorVars[s.health] ?? 'var(--text-muted)';",
    "    const name = s.channel || s.serviceName || getDomain(s.url);",
    "    const dur = Math.floor((now - new Date(s.startTime).getTime()) / 1000);",
    "    const hwBadge = s.hardwareAccelerated ? ' <span title=\"Hardware accelerated\">\\u26A1</span>' : '';",
    "    const showSuffix = s.showName ? ' <span class=\"stream-popover-show\">' + s.showName + '</span>' : '';",
    "    html += '<div class=\"stream-popover-row\">';",
    "    html += '<span class=\"status-dot\" style=\"color: ' + color + ';\">&#9679;</span>';",
    "    html += channelDisplayHtml(s.logoUrl, name, 'stream-popover-logo', 'stream-popover-channel');",
    "    html += hwBadge + showSuffix;",
    "    html += '<span class=\"stream-popover-duration\">' + formatDuration(dur) + '</span>';",
    "    html += '</div>';",
    "  }",
    "  menu.innerHTML = html;",
    "}",

    // Update an already-open stream popover with current data. Called from SSE handlers and the duration interval.
    "function updateStreamPopover() {",
    "  const menu = document.getElementById('stream-popover-menu');",
    "  if(!menu || !menu.classList.contains('show')) return;",
    "  const ids = Object.keys(streamData);",
    "  if(ids.length === 0) {",
    "    menu.classList.remove('show');",
    "    return;",
    "  }",
    "  buildStreamPopoverContent(menu);",
    "}",

    // Toggle the stream popover open or closed.
    "window.toggleStreamPopover = () => {",
    "  const ids = Object.keys(streamData);",
    "  if(ids.length === 0) return;",
    "  const menu = document.getElementById('stream-popover-menu');",
    "  if(!menu) return;",
    "  const isOpen = menu.classList.contains('show');",
    "  dropdowns.close();",
    "  if(!isOpen) {",
    "    buildStreamPopoverContent(menu);",
    "    menu.classList.add('show');",
    "  }",
    "};",

    // Format last issue for display.
    "function formatLastIssue(s) {",
    "  if(!s.lastIssueType || !s.lastIssueTime) { return 'None'; }",
    "  const issueLabel = s.lastIssueType.charAt(0).toUpperCase() + s.lastIssueType.slice(1);",
    "  const timeStr = formatTime(new Date(s.lastIssueTime).toISOString());",
    "  const status = (s.health === 'healthy') ? ' (recovered)' : ' (recovering)';",
    "  return issueLabel + ' at ' + timeStr + status;",
    "}",

    // Format auto-recovery info for display.
    "function formatAutoRecovery(s) {",
    "  const attempts = s.recoveryAttempts;",
    "  const reloads = s.pageReloadsInWindow;",
    "  if(attempts === 0) { return 'N/A'; }",
    "  let str = attempts + (attempts === 1 ? ' attempt' : ' attempts');",
    "  if(reloads > 0) { str += ', ' + reloads + (reloads === 1 ? ' page reload' : ' page reloads'); }",
    "  return str;",
    "}",

    // Format client type breakdown for the detail row.
    "function formatClients(s) {",
    "  if(s.clientCount === 0) { return 'None'; }",
    "  const parts = [];",
    "  for(const c of s.clients) {",
    "    parts.push(c.count + ' ' + (clientTypeLabels[c.type] ?? c.type));",
    "  }",
    "  return parts.join(', ');",
    "}",

    // Render scheduling. All DOM-writing functions funnel through requestAnimationFrame gates so multiple SSE events arriving within the same frame (e.g., two
    // streamHealthChanged events 150ms apart) produce a single DOM write instead of redundant back-to-back rebuilds that destroy and recreate image elements.
    "let tableRenderPending = false;",
    "let popoverRenderPending = false;",

    "function scheduleTableRender() {",
    "  if(!tableRenderPending) {",
    "    tableRenderPending = true;",
    "    requestAnimationFrame(() => { tableRenderPending = false; renderStreamsTable(); });",
    "  }",
    "}",

    "function schedulePopoverRender() {",
    "  if(!popoverRenderPending) {",
    "    popoverRenderPending = true;",
    "    requestAnimationFrame(() => { popoverRenderPending = false; updateStreamPopover(); });",
    "  }",
    "}",

    // Shared cell content renderers. Used by both renderStreamsTable (full rebuild) and updateStreamRow (targeted update) so that each cell's HTML is produced
    // by a single function. This prevents the two rendering paths from diverging - if a cell format changes, the shared function is the only place to update.

    // Health cell: client indicator dot + health badge.
    "function renderHealthCellContent(s) {",
    "  let clientIndicator = '';",
    "  if(s.clientCount > 0) {",
    "    const title = s.clientCount + (s.clientCount !== 1 ? ' clients' : ' client');",
    "    clientIndicator = '<span class=\"client-count\" title=\"' + title + '\">&#9673; ' + s.clientCount + '</span> ';",
    "  }",
    "  return clientIndicator + getHealthBadge(s.health, s.escalationLevel);",
    "}",

    // Detail panel: codec line with mode and native quality suffix.
    "function renderDetailCodec(s) {",
    "  const codecLabel = s.captureCodec ? (s.hardwareAccelerated ? '\\u26A1 ' + s.captureCodec : s.captureCodec) :",
    "    (s.streamingMode === 'native' ? 'Native HLS' : 'Unknown');",
    "  const modeLabel = s.streamingMode === 'native' ? 'Native HLS' : 'Capture';",
    "  let qualitySuffix = '';",
    "  if(s.streamingMode === 'native') {",
    "    const qParts = [];",
    "    if(s.nativeBandwidth > 0) { qParts.push((s.nativeBandwidth / 1000000).toFixed(1) + 'Mbps'); }",
    "    if(s.nativeResolution) {",
    "      const h = s.nativeResolution.split('x')[1];",
    "      qParts.push(nativeResolutionLabels[h] ?? s.nativeResolution);",
    "    }",
    "    if(qParts.length > 0) { qualitySuffix = ' - ' + qParts.join(' '); }",
    "  }",
    "  return '<strong>Codec:</strong> ' + codecLabel + ' (' + modeLabel + ')' + qualitySuffix;",
    "}",

    // Detail panel: started time with optional client count suffix.
    "function renderDetailStarted(s) {",
    "  const clientSuffix = s.clientCount > 0 ? ' &middot; ' + formatClients(s) : '';",
    "  return '<strong>Started:</strong> ' + formatTime(s.startTime) + clientSuffix;",
    "}",

    // Render the streams table.
    "function renderStreamsTable() {",
    "  const tbody = document.getElementById('streams-tbody');",
    "  if(!tbody) return;",
    "  const entries = Object.entries(streamData);",
    "  if(entries.length === 0) {",
    "    tbody.innerHTML = '<tr class=\"empty-row\"><td colspan=\"4\">No active streams</td></tr>';",
    "    return;",
    "  }",
    "  let html = '';",
    "  for(const [ id, s ] of entries) {",
    "    const isExpanded = expandedStreams[id];",
    "    const chevron = isExpanded ? '&#9660;' : '&#9654;';",
    "    const rowTint = rowTints[s.health] ?? 'transparent';",
    "    const channelText = s.channel || s.serviceName || getDomain(s.url);",
    "    const channelDisplay = channelDisplayHtml(s.logoUrl, channelText, 'channel-logo', 'channel-text');",
    "    html += '<tr class=\"stream-row\" data-id=\"' + id + '\" onclick=\"toggleStreamDetails(' + id + ')\" style=\"background-color: ' + rowTint + ';\">';",
    "    html += '<td class=\"chevron\">' + chevron + '</td>';",
    "    const hwIcon = s.hardwareAccelerated ? '\\u26A1 ' : '';",
    "    let nativeBadge = '';",
    "    if(s.streamingMode === 'native') {",
    "      nativeBadge = ' <span class=\"native-badge\" title=\"Native HLS\">Native</span>';",
    "    } else if(s.hardwareAccelerated) {",
    "      nativeBadge = ' <span class=\"native-badge\" title=\"Hardware accelerated\">' + hwIcon + (s.captureCodec || '') + '</span>';",
    "    }",
    "    const durationSpan = '<span class=\"stream-duration\" id=\"duration-' + id + '\">\\u00b7 ' + formatDuration(s.duration) + '</span>';",
    "    html += '<td class=\"stream-info\">' + channelDisplay + nativeBadge + ' ' + durationSpan + '</td>';",
    "    const showDisplay = s.showName ? s.showName : '';",
    "    html += '<td class=\"stream-show\">' + showDisplay + '</td>';",
    "    html += '<td class=\"stream-health\">' + renderHealthCellContent(s) + '</td>';",
    "    html += '</tr>';",
    "    if(isExpanded) {",
    "      html += '<tr class=\"stream-details\" data-id=\"' + id + '\">';",
    "      html += '<td colspan=\"4\">';",
    "      html += '<div class=\"details-content\">';",
    "      html += '<div class=\"details-header\">';",
    "      html += '<div class=\"details-url\">' + s.url + '</div>';",
    "      html += '<div class=\"details-started\">' + renderDetailStarted(s) + '</div>';",
    "      html += '</div>';",
    "      html += '<div class=\"details-metrics\">';",
    "      html += '<div class=\"details-codec\">' + renderDetailCodec(s) + '</div>';",
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

    // Targeted update for a single stream row. Updates only the cells that change between health ticks (health badge, show name, client count, row tint, and
    // detail panel metrics if expanded). Leaves the logo, channel name, badge, and structural elements untouched so image elements are never destroyed and
    // recreated. Falls back to a full table render if the row doesn't exist yet (e.g., race between streamAdded and streamHealthChanged).
    "function updateStreamRow(s) {",
    "  const row = document.querySelector('.stream-row[data-id=\"' + s.id + '\"]');",
    "  if(!row) { scheduleTableRender(); return; }",

    // Row tint.
    "  row.style.backgroundColor = rowTints[s.health] ?? 'transparent';",

    // Health badge cell.
    "  const healthCell = row.querySelector('.stream-health');",
    "  if(healthCell) { healthCell.innerHTML = renderHealthCellContent(s); }",

    // Show name cell.
    "  const showCell = row.querySelector('.stream-show');",
    "  if(showCell) { showCell.textContent = s.showName || ''; }",

    // Detail panel (if expanded). The detail row is a sibling <tr> with the same data-id.
    "  if(expandedStreams[s.id]) {",
    "    const detailRow = document.querySelector('.stream-details[data-id=\"' + s.id + '\"]');",
    "    if(detailRow) {",
    "      const issueEl = detailRow.querySelector('.details-issue');",
    "      if(issueEl) { issueEl.innerHTML = '<strong>Last issue:</strong> ' + formatLastIssue(s); }",
    "      const recoveryEl = detailRow.querySelector('.details-recovery');",
    "      if(recoveryEl) { recoveryEl.innerHTML = '<strong>Recovery:</strong> ' + formatAutoRecovery(s); }",
    "      const memoryEl = detailRow.querySelector('.details-memory');",
    "      if(memoryEl) { memoryEl.innerHTML = '<strong>Memory:</strong> ' + formatBytes(s.memoryBytes); }",
    "      const codecEl = detailRow.querySelector('.details-codec');",
    "      if(codecEl) { codecEl.innerHTML = renderDetailCodec(s); }",
    "      const startedEl = detailRow.querySelector('.details-started');",
    "      if(startedEl) { startedEl.innerHTML = renderDetailStarted(s); }",
    "    }",
    "  }",
    "}",

    // Toggle stream details.
    "function toggleStreamDetails(id) {",
    "  expandedStreams[id] = !expandedStreams[id];",
    "  renderStreamsTable();",
    "}",

    // Update stream durations every second. We calculate duration from the immutable startTime rather than incrementing a counter, ensuring the displayed duration is
    // always accurate regardless of any staleness in server-sent updates.
    "function updateDurations() {",
    "  const now = Date.now();",
    "  for(const [ id, s ] of Object.entries(streamData)) {",
    "    const durationSec = Math.floor((now - new Date(s.startTime).getTime()) / 1000);",
    "    const el = document.getElementById('duration-' + id);",
    "    if(el) el.textContent = '\\u00b7 ' + formatDuration(durationSec);",
    "  }",
    "  schedulePopoverRender();",
    "}",

    // Track the last time any SSE event was received from the status stream. Used by the staleness checker to detect silently dead connections.
    "let lastStatusEventTime = Date.now();",
    "let hiddenSince = 0;",

    // Real-time health indicator updates. These functions update channel health icons and provider login icons in the Channels tab without a page refresh.
    "function formatTimeAgo(ts) {",
    "  const seconds = Math.floor((Date.now() - ts) / 1000);",
    "  if(seconds < 60) { return 'just now'; }",
    "  const minutes = Math.floor(seconds / 60);",
    "  if(minutes < 60) { return minutes + (minutes === 1 ? ' minute ago' : ' minutes ago'); }",
    "  const hours = Math.floor(minutes / 60);",
    "  if(hours < 24) { return hours + (hours === 1 ? ' hour ago' : ' hours ago'); }",
    "  const days = Math.floor(hours / 24);",
    "  return days + (days === 1 ? ' day ago' : ' days ago');",
    "}",
    "function updateChannelHealth(channelKey, status, timestamp, domain) {",
    "  const row = document.getElementById('display-row-' + channelKey);",
    "  if(!row) { return; }",

    // If a domain was supplied (snapshot and real-time events both include it), verify it matches the currently selected domain for this channel. The login button
    // carries a data-auth-domain attribute with the selected domain. If they differ the health entry is from a previous service and should be ignored.
    "  if(domain) {",
    "    const loginBtn = row.querySelector('.btn-icon-login');",
    "    if(loginBtn && loginBtn.getAttribute('data-auth-domain') !== domain) { return; }",
    "  }",
    "  const icon = row.querySelector('.btn-icon-health');",
    "  if(!icon) { return; }",
    "  icon.classList.remove('health-success', 'health-failed');",
    "  if(status === 'success') { icon.classList.add('health-success'); }",
    "  else if(status === 'failed') { icon.classList.add('health-failed'); }",
    "  icon.title = (status === 'success' ? 'Succeeded ' : 'Failed ') + formatTimeAgo(timestamp);",
    "}",
    "function updateDomainAuth(domain, timestamp) {",
    "  const buttons = document.querySelectorAll('.btn-icon-login[data-auth-domain=\"' + domain + '\"]');",
    "  for(const button of buttons) {",
    "    button.classList.add('health-success');",
    "    button.title = 'Verified ' + formatTimeAgo(timestamp);",
    "  }",
    "}",
    "function applyHealthSnapshot(data) {",
    "  for(const [ channelKey, entry ] of Object.entries(data.channels)) {",
    "    updateChannelHealth(channelKey, entry.status, entry.timestamp, entry.domain);",
    "  }",
    "  for(const [ domain, timestamp ] of Object.entries(data.domains)) {",
    "    updateDomainAuth(domain, timestamp);",
    "  }",
    "}",

    // Connect (or reconnect) to the status SSE stream. Closes any existing connection first so this is safe to call repeatedly.
    "function connectStatusSSE() {",
    "  if(statusEventSource) { statusEventSource.close(); }",
    "  statusEventSource = new EventSource('/streams/status');",
    "  lastStatusEventTime = Date.now();",

    // Local helper that registers an event listener and updates the staleness timestamp on every event. Handlers are optional so heartbeat can
    // be registered with just on('heartbeat') for pure keepalive tracking. The onerror handler stays outside this wrapper because errors must
    // not reset the staleness timer - a connection that only fires errors is still dead.
    "  function on(event, handler) {",
    "    statusEventSource.addEventListener(event, (e) => {",
    "      lastStatusEventTime = Date.now();",
    "      if(handler) { handler(e); }",
    "    });",
    "  }",
    "  on('heartbeat');",
    "  on('snapshot', (e) => {",
    "    const data = JSON.parse(e.data);",
    "    systemData = data.system;",
    "    streamData = {};",
    "    for(const stream of data.streams) {",
    "      streamData[stream.id] = stream;",
    "    }",
    "    updateSystemStatus();",
    "    renderStreamsTable();",
    "    updateStreamPopover();",
    "    if(data.health) { applyHealthSnapshot(data.health); }",
    "  });",
    "  on('streamAdded', (e) => {",
    "    const s = JSON.parse(e.data);",
    "    streamData[s.id] = s;",
    "    renderStreamsTable();",
    "    updateStreamPopover();",
    "  });",
    "  on('streamRemoved', (e) => {",
    "    const data = JSON.parse(e.data);",
    "    delete streamData[data.id];",
    "    delete expandedStreams[data.id];",
    "    renderStreamsTable();",
    "    updateStreamPopover();",
    "    if(typeof updateRestartDialogStatus === 'function') {",
    "      updateRestartDialogStatus();",
    "    }",
    "  });",
    "  on('streamHealthChanged', (e) => {",
    "    const s = JSON.parse(e.data);",
    "    const prev = streamData[s.id];",
    "    if(prev) {",
    "      const structuralChange = (prev.logoUrl !== s.logoUrl) || (prev.streamingMode !== s.streamingMode) ||",
    "        (prev.hardwareAccelerated !== s.hardwareAccelerated) || (prev.captureCodec !== s.captureCodec);",
    "      streamData[s.id] = s;",
    "      if(structuralChange) {",
    "        scheduleTableRender();",
    "      } else {",
    "        updateStreamRow(s);",
    "      }",
    "      schedulePopoverRender();",
    "    }",
    "  });",
    "  on('systemStatusChanged', (e) => {",
    "    systemData = JSON.parse(e.data);",
    "    updateSystemStatus();",
    "  });",
    "  on('healthChanged', (e) => {",
    "    const event = JSON.parse(e.data);",
    "    updateChannelHealth(event.channelKey, event.status, event.timestamp, event.domain);",
    "    if(event.status === 'success') { updateDomainAuth(event.domain, event.timestamp); }",
    "  });",

    // Channel table updates from the server (e.g., logo population after startup). Applies the patch via the shared channelTable namespace.
    "  on('channelUpdate', (e) => {",
    "    channelTable.applyPatch(JSON.parse(e.data));",
    "  });",
    "  statusEventSource.onerror = () => {",
    "    document.getElementById('system-health').innerHTML = '<span class=\"status-dot\" style=\"color: var(--stream-stalled);\">&#9679;</span> Updates paused';",
    "  };",
    "}",

    // Initial connection and periodic timers.
    "connectStatusSSE();",
    "setInterval(updateDurations, 1000);",

    // Staleness detection: if no SSE event has arrived in 45 seconds, the connection is likely dead. Reconnect proactively.
    "setInterval(() => {",
    "  if((Date.now() - lastStatusEventTime) > 45000) { connectStatusSSE(); }",
    "}, 45000);",

    // Visibility-driven reconnect. When the page returns from being hidden for more than 30 seconds, reconnect the status stream and re-activate
    // the current tab so the logs stream reconnects naturally through its existing tabactivated listener.
    "document.addEventListener('visibilitychange', () => {",
    "  if(document.hidden) {",
    "    hiddenSince = Date.now();",
    "  } else if((hiddenSince > 0) && ((Date.now() - hiddenSince) > 30000)) {",
    "    hiddenSince = 0;",
    "    connectStatusSSE();",
    "    const activeTab = document.querySelector('.tab-btn.active');",
    "    if(activeTab) {",
    "      document.dispatchEvent(new CustomEvent('tabactivated', { detail: { category: activeTab.getAttribute('data-category') } }));",
    "    }",
    "  } else {",
    "    hiddenSince = 0;",
    "  }",
    "});",

    // Copy playlist URL function for Overview tab Quick Start section. Delegates to the shared copyToClipboard utility for consistent clipboard handling
    // across all contexts (HTTPS Clipboard API with execCommand fallback for plain HTTP).
    "window.copyOverviewPlaylistUrl = () => {",
    "  const urlEl = document.getElementById('overview-playlist-url');",
    "  if(urlEl) { copyToClipboard(urlEl.textContent, 'Playlist URL copied to clipboard.'); }",
    "};",

    // JS-based tooltips for devices where the primary input can't hover (iPadOS). Safari on iPadOS doesn't show native title tooltips, so we use
    // a single <div> appended to <body> and positioned via getBoundingClientRect(). This is immune to overflow containers and stacking contexts.
    // On pure-touch devices without a trackpad, mouseenter never fires so the tooltip stays hidden. Desktop skips initialization entirely.
    "(function() {",
    "  if(!window.matchMedia('(hover: none)').matches) return;",
    "  const tip = document.createElement('div');",
    "  tip.className = 'btn-icon-tooltip';",
    "  document.body.appendChild(tip);",
    "  document.addEventListener('mouseenter', (e) => {",
    "    const btn = e.target.closest('.btn-icon[aria-label]');",
    "    if(!btn) return;",
    "    const label = btn.getAttribute('title') || btn.getAttribute('aria-label');",
    "    if(!label) return;",
    "    const rect = btn.getBoundingClientRect();",
    "    tip.textContent = label;",
    "    tip.classList.add('visible');",
    "    tip.style.top = (rect.bottom + 6 + (window.scrollY || 0)) + 'px';",
    "    tip.style.left = (rect.left + rect.width / 2 + (window.scrollX || 0)) + 'px';",
    "    tip.style.transform = 'translateX(-50%)';",
    "  }, true);",
    "  document.addEventListener('mouseleave', (e) => {",
    "    const src = e.target.closest('.btn-icon[aria-label]');",
    "    if(!src) return;",
    "    const dest = e.relatedTarget?.closest?.('.btn-icon[aria-label]');",
    "    if(dest === src) return;",
    "    tip.classList.remove('visible');",
    "  }, true);",
    "})();",

    "</script>"
  ].join("\n");
}
