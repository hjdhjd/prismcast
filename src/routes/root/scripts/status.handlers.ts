/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * status.handlers.ts: Pure handler logic for the PrismCast status display script.
 *
 * The architecture is ports-and-adapters with the same shape used across upgrade/, service/, utils/ffmpeg.ts, and utils/clock.ts. Every formatter, renderer, DOM
 * mutator, SSE event handler, and lifecycle helper that the status display needs is defined here as a free-standing TypeScript function over a HandlerContext.
 *
 *   - HandlerContext (this file) - the runtime-input port; the document, the mutable client state, and the window.*-resolved externals
 *   - ClientState (this file) - the mutable record of streamData / systemData / expandedStreams / staleness counters / rAF gates
 *   - ClientExternals (this file) - the readonly handle to sibling-script window.* APIs (channelTable, dropdowns, copyToClipboard, etc.)
 *   - HANDLER_CONSTANTS (this file) - the registry of script-side constants (health color CSS vars, label maps, row tints) emitted into the script body
 *   - HANDLER_FUNCTIONS (this file) - the registry of pure functions whose .toString() output is concatenated into the emitted script body
 *   - generateStatusScript (status.ts) - the boundary; the only place that builds strings, that wires window.*, and that constructs the EventSource
 *
 * Why this file exists. Free-standing handler functions are directly importable and callable from Node tests with synthetic context literals, which avoids the
 * happy-dom EventSource gap that would otherwise force every SSE behavior test to run inside a synthetic DOM. The IIFE shell in status.ts is a thin glue layer;
 * the test surface is this handler module.
 *
 * Single source of truth. Every function defined here is consumed twice - once by Node tests that import and call it directly, and once by the browser via
 * Function.prototype.toString() concatenation in generateStatusScript(). The TypeScript function IS the source; there is no parallel hand-mirrored implementation
 * to drift away from it. The constraint this places on function bodies is that they may reference only their parameters, browser globals (document, JSON, Date,
 * Math, URL, Object, Number, requestAnimationFrame, setInterval, EventSource, window, CustomEvent), or sibling functions that are also emitted. No imports, no
 * closures over module-scope TS variables, no Node-only APIs. The constants are emitted via JSON.stringify so handler bodies can reference them by their TS-side
 * identifier.
 */

/**
 * The shape of a single stream as carried over the SSE wire and stored in the client state. This is the script-side projection - only the fields the renderers
 * and handlers actually read are declared. The server-side StreamListItem and statusEmitter payloads are richer; the script consumes a subset.
 */
export interface StreamSummary {

  readonly captureCodec?: string;
  readonly channel?: string | null;
  readonly clientCount: number;
  readonly clients: readonly { readonly count: number; readonly type: string }[];
  readonly duration: number;
  readonly escalationLevel?: number;
  readonly hardwareAccelerated?: boolean;
  readonly health: "buffering" | "error" | "healthy" | "recovering" | "stalled";
  readonly id: number | string;
  readonly lastIssueTime?: string;
  readonly lastIssueType?: string;
  readonly logoUrl?: string;
  readonly memoryBytes: number;
  readonly nativeBandwidth?: number;
  readonly nativeResolution?: string;
  readonly pageReloadsInWindow: number;
  readonly recoveryAttempts: number;
  readonly serviceName?: string;
  readonly showName?: string;
  readonly startTime: string;
  readonly streamingMode?: "capture" | "native";
  readonly url: string;
}

/**
 * The shape of the system-status payload (snapshot.system and systemStatusChanged events). Subset of the server-side HealthStatus; only what updateSystemStatus
 * reads.
 */
export interface SystemSummary {

  readonly browser: { readonly connected: boolean };
  readonly streams: { readonly active: number; readonly limit: number };
}

/**
 * The shape of the snapshot SSE payload. snapshot is the only event that delivers the full state in one message; subsequent events are deltas.
 */
export interface SnapshotPayload {

  readonly health?: HealthSnapshot;
  readonly streams: readonly StreamSummary[];
  readonly system: SystemSummary;
}

/**
 * The shape of the streamRemoved SSE payload. Just the id.
 */
export interface StreamRemovedPayload {

  readonly id: number | string;
}

/**
 * The shape of the healthChanged SSE payload. Carries the per-channel update plus the domain that owns the auth context.
 */
export interface HealthChangedPayload {

  readonly channelKey: string;
  readonly domain?: string;
  readonly status: "failed" | "success";
  readonly timestamp: number;
}

/**
 * Health snapshot delivered inside the snapshot event. Channels and domains are independent maps keyed by their respective identifiers.
 */
export interface HealthSnapshot {

  readonly channels: Readonly<Record<string, { readonly domain?: string; readonly status: "failed" | "success"; readonly timestamp: number }>>;
  readonly domains: Readonly<Record<string, number>>;
}

/**
 * Mutable client state. The IIFE shell constructs one of these at startup; every handler reads and writes it via ctx.state. Handlers mutate ctx.state fields
 * directly; mutation methods would add no value over field assignment.
 */
export interface ClientState {

  expandedStreams: Record<string, boolean>;
  hiddenSince: number;
  lastStatusEventTime: number;
  popoverRenderPending: boolean;
  streamData: Record<string, StreamSummary>;
  systemData: SystemSummary | null;
  tableRenderPending: boolean;
}

/**
 * Readonly handle to sibling-script window.* APIs. Resolved at IIFE construction time; tests pass stub literals. updateRestartDialogStatus is read via a getter
 * in the IIFE so the streamRemoved handler picks up later registration from config.ts (the script that defines it loads after status.ts in the page).
 */
export interface ClientExternals {

  readonly channelDisplayHtml: (logoUrl: string | undefined, name: string, logoClass: string, textClass: string) => string;
  readonly channelTable: { readonly applyPatch: (patch: unknown) => void };
  readonly copyToClipboard: (text: string, message: string) => void;
  readonly dropdowns: { readonly close: () => void };
  readonly updateRestartDialogStatus: (() => void) | undefined;
}

/**
 * The runtime-input port every handler consumes. document is the DOM; state is the mutable record; externals are the sibling-script window.* APIs. Decision
 * logic is a pure function of this shape - production wires it inside the IIFE (constructed by generateStatusScript), tests pass a context literal.
 */
export interface HandlerContext {

  readonly document: Document;
  readonly externals: ClientExternals;
  readonly state: ClientState;
}

/**
 * Builds an empty client state. The IIFE calls this at startup; tests call it for fixtures. Defaults to "no streams, no system data, watchdog timestamps zeroed,
 * rAF gates open." lastStatusEventTime is zeroed here; the IIFE reseeds it to Date.now() right before opening the EventSource so the staleness watchdog has a
 * meaningful baseline. Tests that exercise the watchdog seed it explicitly.
 */
export function createInitialState(): ClientState {

  return {

    expandedStreams: {},
    hiddenSince: 0,
    lastStatusEventTime: 0,
    popoverRenderPending: false,
    streamData: {},
    systemData: null,
    tableRenderPending: false
  };
}

// CSS-variable-backed color tokens for the health-state badge dot. The recovering/stalled/etc. literals are the discriminants the server emits. Typed as a wide
// string-record so script-side handlers can index by any health value with a `?? fallback` pattern (consistent with how the other lookup tables below are
// structured).
const healthColorVars: Record<string, string> = {

  buffering: "var(--stream-buffering)",
  error: "var(--stream-error)",
  healthy: "var(--stream-healthy)",
  recovering: "var(--stream-recovering)",
  stalled: "var(--stream-stalled)"
};

// Static labels for each top-level health state. The recovering state delegates to getRecoveringLabel which dispatches on the escalation level.
const healthLabels: Record<string, string> = {

  buffering: "Buffering",
  error: "Error",
  healthy: "Healthy",
  stalled: "Stalled"
};

// Client-type display labels for the detail row's client breakdown. Hoisted to module scope so formatClients does not reallocate the map on every render.
const clientTypeLabels: Record<string, string> = {

  hls: "HLS",
  mpegts: "MPEG-TS"
};

// Native HLS variant resolution display labels. Hoisted so renderStreamsTable does not reallocate on every stream row.
const nativeResolutionLabels: Record<string, string> = {

  "1080": "1080p",
  "2160": "4K",
  "360": "360p",
  "480": "480p",
  "720": "720p"
};

// Row background tints keyed by health state. Module-scope constant so renderStreamsTable reads from a single shared map instead of allocating a fresh object
// for every stream row. Consistent with clientTypeLabels and nativeResolutionLabels above - accessed inline at the call site, no wrapper helper.
const rowTints: Record<string, string> = {

  buffering: "var(--stream-tint-buffering)",
  error: "var(--stream-tint-error)",
  healthy: "transparent",
  recovering: "var(--stream-tint-recovering)",
  stalled: "var(--stream-tint-stalled)"
};

/**
 * The script-side constant registry. generateStatusScript() emits each entry as `const NAME = <JSON.stringify(value)>;`. Naming is the IIFE-side identifier
 * (matches the closure references inside the function bodies below). Adding a new constant means appending here and using its name in handler bodies; nothing
 * else changes.
 */
export const HANDLER_CONSTANTS: readonly { readonly name: string; readonly value: unknown }[] = [

  { name: "clientTypeLabels", value: clientTypeLabels },
  { name: "healthColorVars", value: healthColorVars },
  { name: "healthLabels", value: healthLabels },
  { name: "nativeResolutionLabels", value: nativeResolutionLabels },
  { name: "rowTints", value: rowTints }
];

// Pure formatters. Free of DOM and free of state; only depend on their parameters. Tests call them directly with literal inputs.

// Format a duration in seconds to a human-readable label. Threshold ladder: <60s -> seconds, <3600s -> minutes+seconds, otherwise hours+minutes.
function formatDuration(seconds: number): string {

  if(seconds < 60) {

    return String(seconds) + "s";
  }

  if(seconds < 3600) {

    return String(Math.floor(seconds / 60)) + "m " + String(seconds % 60) + "s";
  }

  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);

  return String(h) + "h " + String(m) + "m";
}

// Format a byte count to a human-readable label. KB/MB are 1024-based. Used for the per-stream segment-buffer memory line.
function formatBytes(bytes: number): string {

  if(bytes === 0) {

    return "0 B";
  }

  if(bytes < 1024) {

    return String(bytes) + " B";
  }

  if(bytes < 1048576) {

    return (bytes / 1024).toFixed(1) + " KB";
  }

  return (bytes / 1048576).toFixed(1) + " MB";
}

// Format an absolute time (e.g., "6:54 AM" same-day, or "Jan 14, 6:54 AM" cross-day). 12-hour clock; no seconds.
function formatTime(isoString: string): string {

  const date = new Date(isoString);
  const now = new Date();
  let hours = date.getHours();
  const minutes = date.getMinutes();
  const ampm = hours >= 12 ? "PM" : "AM";

  hours = hours % 12;
  hours = hours ? hours : 12;

  let timeStr = String(hours) + ":" + (minutes < 10 ? "0" : "") + String(minutes) + " " + ampm;

  if(date.toDateString() !== now.toDateString()) {

    const months = [ "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" ];

    timeStr = String(months[date.getMonth()]) + " " + String(date.getDate()) + ", " + timeStr;
  }

  return timeStr;
}

// Format a timestamp as "N {unit}s ago" relative to now. Used by the channel/domain health icons in the Channels tab to show how recent the last result was.
function formatTimeAgo(ts: number): string {

  const seconds = Math.floor((Date.now() - ts) / 1000);

  if(seconds < 60) {

    return "just now";
  }

  const minutes = Math.floor(seconds / 60);

  if(minutes < 60) {

    return String(minutes) + (minutes === 1 ? " minute ago" : " minutes ago");
  }

  const hours = Math.floor(minutes / 60);

  if(hours < 24) {

    return String(hours) + (hours === 1 ? " hour ago" : " hours ago");
  }

  const days = Math.floor(hours / 24);

  return String(days) + (days === 1 ? " day ago" : " days ago");
}

// Extract a concise display domain from a URL (last two hostname parts). Mirrors the server-side extractDomain() in utils/format.ts so the popover label and the
// table fallback agree on what "the domain" means.
function getDomain(url: string): string {

  const parsed = URL.parse(url);

  if(!parsed) {

    return url;
  }

  const parts = parsed.hostname.split(".");

  return (parts.length > 2) ? parts.slice(-2).join(".") : parts.join(".");
}

// Resolve the recovery-level label for a stream in the recovering state. Kept as a dedicated helper because the mapping is level-based, not string-based, so it
// cannot fold into the same lookup map as the top-level health labels. Escalation level semantics defined in monitor.ts: L1=play/unmute, L2=seek, L3=source
// reload, L4=page navigation.
function getRecoveringLabel(level: number): string {

  switch(level) {

    case 1: return "Resuming playback";

    case 2: return "Syncing to live";

    case 3: return "Reloading player";

    default: return (level >= 4) ? "Reloading page" : "Recovering";
  }
}

// Get health badge HTML using CSS variables for theme-aware colors.
function getHealthBadge(health: string, level: number): string {

  const label = (health === "recovering") ? getRecoveringLabel(level) : (healthLabels[health] ?? health);

  return "<span class=\"status-dot\" style=\"color: " + (healthColorVars[health] ?? "var(--text-muted)") + ";\">&#9679;</span> " +
    "<span style=\"color: var(--text-secondary);\">" + label + "</span>";
}

// Format the per-stream "last issue" line for the detail panel. Returns "None" for streams that have not yet hit a health event; otherwise an "{Issue} at {time}
// ({status})" form where status is "(recovered)" if currently healthy or "(recovering)" otherwise.
function formatLastIssue(s: StreamSummary): string {

  if(!s.lastIssueType || !s.lastIssueTime) {

    return "None";
  }

  const issueLabel = s.lastIssueType.charAt(0).toUpperCase() + s.lastIssueType.slice(1);
  const timeStr = formatTime(new Date(s.lastIssueTime).toISOString());
  const status = (s.health === "healthy") ? " (recovered)" : " (recovering)";

  return issueLabel + " at " + timeStr + status;
}

// Format the per-stream "auto-recovery" line for the detail panel. "N/A" when no attempts have happened; otherwise a count and an optional page-reload tail.
function formatAutoRecovery(s: StreamSummary): string {

  const attempts = s.recoveryAttempts;
  const reloads = s.pageReloadsInWindow;

  if(attempts === 0) {

    return "N/A";
  }

  let str = String(attempts) + (attempts === 1 ? " attempt" : " attempts");

  if(reloads > 0) {

    str += ", " + String(reloads) + (reloads === 1 ? " page reload" : " page reloads");
  }

  return str;
}

// Format the client breakdown for the detail row. "None" when the stream has zero clients; otherwise "{count} {type}" entries joined by commas.
function formatClients(s: StreamSummary): string {

  if(s.clientCount === 0) {

    return "None";
  }

  const parts: string[] = [];

  for(const c of s.clients) {

    parts.push(String(c.count) + " " + (clientTypeLabels[c.type] ?? c.type));
  }

  return parts.join(", ");
}

// Health cell HTML: client-count indicator (when clients > 0) followed by the health badge. Shared between the full-table render and the targeted-row update so
// the two paths always produce identical health cells.
function renderHealthCellContent(s: StreamSummary): string {

  let clientIndicator = "";

  if(s.clientCount > 0) {

    const title = String(s.clientCount) + (s.clientCount !== 1 ? " clients" : " client");

    clientIndicator = "<span class=\"client-count\" title=\"" + title + "\">&#9673; " + String(s.clientCount) + "</span> ";
  }

  return clientIndicator + getHealthBadge(s.health, s.escalationLevel ?? 0);
}

// Detail panel codec line: "{Codec} ({Mode}){suffix}" where suffix carries native-HLS bandwidth and resolution if present. Hardware-accelerated codecs prefix
// the lightning bolt; native HLS without a captureCodec fills "Native HLS" instead.
function renderDetailCodec(s: StreamSummary): string {

  const codecLabel = s.captureCodec ? (s.hardwareAccelerated ? "⚡ " + s.captureCodec : s.captureCodec) :
    (s.streamingMode === "native" ? "Native HLS" : "Unknown");
  const modeLabel = s.streamingMode === "native" ? "Native HLS" : "Capture";
  let qualitySuffix = "";

  if(s.streamingMode === "native") {

    const qParts: string[] = [];

    if((s.nativeBandwidth ?? 0) > 0) {

      qParts.push(((s.nativeBandwidth ?? 0) / 1000000).toFixed(1) + "Mbps");
    }

    if(s.nativeResolution) {

      const h = s.nativeResolution.split("x")[1];

      qParts.push((h ? nativeResolutionLabels[h] : undefined) ?? s.nativeResolution);
    }

    if(qParts.length > 0) {

      qualitySuffix = " - " + qParts.join(" ");
    }
  }

  return "<strong>Codec:</strong> " + codecLabel + " (" + modeLabel + ")" + qualitySuffix;
}

// Detail panel "Started:" line with optional "(N HLS, M MPEG-TS)" client suffix when clients > 0.
function renderDetailStarted(s: StreamSummary): string {

  const clientSuffix = s.clientCount > 0 ? " &middot; " + formatClients(s) : "";

  return "<strong>Started:</strong> " + formatTime(s.startTime) + clientSuffix;
}

// DOM mutators. Each takes a HandlerContext (and any per-call inputs) and reads/writes ctx.document and ctx.state. Tests call them directly against happy-dom
// or stub documents.

// Update the system-status display in the page header. Shows browser-connected status (green dot when connected, red dot + "Browser offline" when not) plus a
// stream count summary.
function updateSystemStatus(ctx: HandlerContext): void {

  if(!ctx.state.systemData) {

    return;
  }

  const healthEl = ctx.document.getElementById("system-health");
  const streamEl = ctx.document.getElementById("stream-count");

  if(!healthEl || !streamEl) {

    return;
  }

  if(ctx.state.systemData.browser.connected) {

    healthEl.innerHTML = "<span class=\"status-dot\" style=\"color: var(--stream-healthy);\">&#9679;</span>";
  } else {

    healthEl.innerHTML = "<span class=\"status-dot\" style=\"color: var(--stream-error);\">&#9679;</span> Browser offline";
  }

  const active = ctx.state.systemData.streams.active;
  const limit = ctx.state.systemData.streams.limit;

  if(active === 0) {

    streamEl.textContent = "0 streams";
    streamEl.classList.remove("clickable");

    const popMenu = ctx.document.getElementById("stream-popover-menu");

    if(popMenu) {

      popMenu.classList.remove("show");
    }
  } else {

    streamEl.textContent = String(active) + "/" + String(limit) + " streams";
    streamEl.classList.add("clickable");
  }
}

// Build the stream popover content from streamData. Populates the supplied menu element with one row per active stream.
function buildStreamPopoverContent(menu: Element, ctx: HandlerContext): void {

  let html = "";
  const now = Date.now();

  for(const s of Object.values(ctx.state.streamData)) {

    // The fallback chain uses || (not ??) so empty-string channel/serviceName values fall through to getDomain. The server may emit an empty string when the
    // channel is not yet identified.
    const color = healthColorVars[s.health] ?? "var(--text-muted)";
    const name = (s.channel ?? "") || (s.serviceName ?? "") || getDomain(s.url);
    const dur = Math.floor((now - new Date(s.startTime).getTime()) / 1000);
    const hwBadge = s.hardwareAccelerated ? " <span title=\"Hardware accelerated\">⚡</span>" : "";
    const showSuffix = s.showName ? " <span class=\"stream-popover-show\">" + s.showName + "</span>" : "";

    html += "<div class=\"stream-popover-row\">";
    html += "<span class=\"status-dot\" style=\"color: " + color + ";\">&#9679;</span>";
    html += ctx.externals.channelDisplayHtml(s.logoUrl, name, "stream-popover-logo", "stream-popover-channel");
    html += hwBadge + showSuffix;
    html += "<span class=\"stream-popover-duration\">" + formatDuration(dur) + "</span>";
    html += "</div>";
  }

  menu.innerHTML = html;
}

// Refresh an already-open stream popover with current data. No-op if the popover is closed or there are no streams (in which case it auto-closes).
function updateStreamPopover(ctx: HandlerContext): void {

  const menu = ctx.document.getElementById("stream-popover-menu");

  if(!menu?.classList.contains("show")) {

    return;
  }

  const ids = Object.keys(ctx.state.streamData);

  if(ids.length === 0) {

    menu.classList.remove("show");

    return;
  }

  buildStreamPopoverContent(menu, ctx);
}

// Render scheduling. The DOM-writing render functions funnel through requestAnimationFrame gates so multiple SSE events arriving within the same frame produce a
// single DOM write instead of redundant back-to-back rebuilds that destroy and recreate image elements.

// Schedule a full-table render on the next animation frame. Idempotent within a frame - repeated calls collapse into one render.
function scheduleTableRender(ctx: HandlerContext): void {

  if(!ctx.state.tableRenderPending) {

    ctx.state.tableRenderPending = true;
    requestAnimationFrame(() => {

      ctx.state.tableRenderPending = false;
      renderStreamsTable(ctx);
    });
  }
}

// Schedule a popover refresh on the next animation frame. Idempotent within a frame.
function schedulePopoverRender(ctx: HandlerContext): void {

  if(!ctx.state.popoverRenderPending) {

    ctx.state.popoverRenderPending = true;
    requestAnimationFrame(() => {

      ctx.state.popoverRenderPending = false;
      updateStreamPopover(ctx);
    });
  }
}

// Render the full streams table. Used by the snapshot/streamAdded/streamRemoved handlers and as the fallback when updateStreamRow cannot find an existing row.
function renderStreamsTable(ctx: HandlerContext): void {

  const tbody = ctx.document.getElementById("streams-tbody");

  if(!tbody) {

    return;
  }

  const entries = Object.entries(ctx.state.streamData);

  if(entries.length === 0) {

    tbody.innerHTML = "<tr class=\"empty-row\"><td colspan=\"4\">No active streams</td></tr>";

    return;
  }

  let html = "";

  for(const [ id, s ] of entries) {

    const isExpanded = ctx.state.expandedStreams[id];
    const chevron = isExpanded ? "&#9660;" : "&#9654;";
    const rowTint = rowTints[s.health] ?? "transparent";
    // The channel/serviceName fallback uses || semantics so empty strings (server may emit them when identification is pending) fall through to the domain.
    const channelText = (s.channel ?? "") || (s.serviceName ?? "") || getDomain(s.url);
    const channelDisplay = ctx.externals.channelDisplayHtml(s.logoUrl, channelText, "channel-logo", "channel-text");

    html += "<tr class=\"stream-row\" data-id=\"" + id + "\" data-click-action=\"toggle-stream-details\" data-stream-id=\"" + id +
      "\" style=\"background-color: " + rowTint + ";\">";
    html += "<td class=\"chevron\">" + chevron + "</td>";

    const hwIcon = s.hardwareAccelerated ? "⚡ " : "";
    let nativeBadge = "";

    if(s.streamingMode === "native") {

      nativeBadge = " <span class=\"native-badge\" title=\"Native HLS\">Native</span>";
    } else if(s.hardwareAccelerated) {

      nativeBadge = " <span class=\"native-badge\" title=\"Hardware accelerated\">" + hwIcon + (s.captureCodec ?? "") + "</span>";
    }

    const durationSpan = "<span class=\"stream-duration\" id=\"duration-" + id + "\">· " + formatDuration(s.duration) + "</span>";

    html += "<td class=\"stream-info\">" + channelDisplay + nativeBadge + " " + durationSpan + "</td>";

    const showDisplay = s.showName ?? "";

    html += "<td class=\"stream-show\">" + showDisplay + "</td>";
    html += "<td class=\"stream-health\">" + renderHealthCellContent(s) + "</td>";
    html += "</tr>";

    if(isExpanded) {

      html += "<tr class=\"stream-details\" data-id=\"" + id + "\">";
      html += "<td colspan=\"4\">";
      html += "<div class=\"details-content\">";
      html += "<div class=\"details-header\">";
      html += "<div class=\"details-url\">" + s.url + "</div>";
      html += "<div class=\"details-started\">" + renderDetailStarted(s) + "</div>";
      html += "</div>";
      html += "<div class=\"details-metrics\">";
      html += "<div class=\"details-codec\">" + renderDetailCodec(s) + "</div>";
      html += "<div class=\"details-issue\"><strong>Last issue:</strong> " + formatLastIssue(s) + "</div>";
      html += "<div class=\"details-recovery\"><strong>Recovery:</strong> " + formatAutoRecovery(s) + "</div>";
      html += "<div class=\"details-memory\"><strong>Memory:</strong> " + formatBytes(s.memoryBytes) + "</div>";
      html += "</div>";
      html += "</div>";
      html += "</td></tr>";
    }
  }

  tbody.innerHTML = html;
}

// Targeted update for a single stream row. Updates only the cells that change between health ticks (health badge, show name, client count, row tint, and detail
// panel metrics if expanded). Leaves the logo, channel name, badge, and structural elements untouched so image elements are never destroyed and recreated. Falls
// back to a full table render if the row does not exist yet (e.g., race between streamAdded and streamHealthChanged).
function updateStreamRow(s: StreamSummary, ctx: HandlerContext): void {

  const row = ctx.document.querySelector<HTMLElement>(".stream-row[data-id=\"" + String(s.id) + "\"]");

  if(!row) {

    scheduleTableRender(ctx);

    return;
  }

  row.style.backgroundColor = rowTints[s.health] ?? "transparent";

  const healthCell = row.querySelector(".stream-health");

  if(healthCell) {

    healthCell.innerHTML = renderHealthCellContent(s);
  }

  const showCell = row.querySelector(".stream-show");

  if(showCell) {

    showCell.textContent = s.showName ?? "";
  }

  if(ctx.state.expandedStreams[s.id]) {

    const detailRow = ctx.document.querySelector(".stream-details[data-id=\"" + String(s.id) + "\"]");

    if(detailRow) {

      const issueEl = detailRow.querySelector(".details-issue");

      if(issueEl) {

        issueEl.innerHTML = "<strong>Last issue:</strong> " + formatLastIssue(s);
      }

      const recoveryEl = detailRow.querySelector(".details-recovery");

      if(recoveryEl) {

        recoveryEl.innerHTML = "<strong>Recovery:</strong> " + formatAutoRecovery(s);
      }

      const memoryEl = detailRow.querySelector(".details-memory");

      if(memoryEl) {

        memoryEl.innerHTML = "<strong>Memory:</strong> " + formatBytes(s.memoryBytes);
      }

      const codecEl = detailRow.querySelector(".details-codec");

      if(codecEl) {

        codecEl.innerHTML = renderDetailCodec(s);
      }

      const startedEl = detailRow.querySelector(".details-started");

      if(startedEl) {

        startedEl.innerHTML = renderDetailStarted(s);
      }
    }
  }
}

// Toggle the expanded/collapsed state of a stream's detail panel. Triggered by inline onclick from the table row; the IIFE exposes a window-bound trampoline.
function toggleStreamDetails(id: number | string, ctx: HandlerContext): void {

  ctx.state.expandedStreams[id] = !ctx.state.expandedStreams[id];
  renderStreamsTable(ctx);
}

// Refresh all stream-duration cells. Computed from the immutable startTime so the displayed duration is always accurate regardless of any staleness in
// server-sent updates. Also schedules a popover refresh so the popover's duration column ticks alongside the table.
function updateDurations(ctx: HandlerContext): void {

  const now = Date.now();

  for(const [ id, s ] of Object.entries(ctx.state.streamData)) {

    const durationSec = Math.floor((now - new Date(s.startTime).getTime()) / 1000);
    const el = ctx.document.getElementById("duration-" + id);

    if(el) {

      el.textContent = "· " + formatDuration(durationSec);
    }
  }

  schedulePopoverRender(ctx);
}

// Update the per-channel health icon in the Channels tab. Domain match check guards against stale events from a prior service binding - the login button carries
// a data-auth-domain attribute that must agree with the event's domain (when supplied) for the update to apply.
function updateChannelHealth(channelKey: string, status: "failed" | "success", timestamp: number, domain: string | undefined, ctx: HandlerContext): void {

  const row = ctx.document.getElementById("display-row-" + channelKey);

  if(!row) {

    return;
  }

  if(domain) {

    const loginBtn = row.querySelector(".btn-icon-login");

    if(loginBtn && (loginBtn.getAttribute("data-auth-domain") !== domain)) {

      return;
    }
  }

  const icon = row.querySelector<HTMLElement>(".btn-icon-health");

  if(!icon) {

    return;
  }

  icon.classList.remove("health-success", "health-failed");

  if(status === "success") {

    icon.classList.add("health-success");
  } else {

    icon.classList.add("health-failed");
  }

  icon.title = (status === "success" ? "Succeeded " : "Failed ") + formatTimeAgo(timestamp);
}

// Mark every login button bound to the given domain as verified, with a "Verified {time-ago}" tooltip. Called for successful health events and for snapshot
// domain entries.
function updateDomainAuth(domain: string, timestamp: number, ctx: HandlerContext): void {

  const buttons = ctx.document.querySelectorAll<HTMLElement>(".btn-icon-login[data-auth-domain=\"" + domain + "\"]");

  for(const button of Array.from(buttons)) {

    button.classList.add("health-success");
    button.title = "Verified " + formatTimeAgo(timestamp);
  }
}

// Apply a full health snapshot. Walks the channels map then the domains map, applying the per-key updaters for each entry.
function applyHealthSnapshot(data: HealthSnapshot, ctx: HandlerContext): void {

  for(const [ channelKey, entry ] of Object.entries(data.channels)) {

    updateChannelHealth(channelKey, entry.status, entry.timestamp, entry.domain, ctx);
  }

  for(const [ domain, timestamp ] of Object.entries(data.domains)) {

    updateDomainAuth(domain, timestamp, ctx);
  }
}

// SSE handlers. Each takes the parsed event payload and the HandlerContext. The IIFE wires JSON.parse and event-listener registration; the handlers themselves
// are pure logic.

// Snapshot handler. Replaces all client state with the snapshot's contents and re-renders everything, then applies the optional health snapshot.
function handleSnapshot(data: SnapshotPayload, ctx: HandlerContext): void {

  ctx.state.systemData = data.system;
  ctx.state.streamData = {};

  for(const stream of data.streams) {

    ctx.state.streamData[stream.id] = stream;
  }

  updateSystemStatus(ctx);
  renderStreamsTable(ctx);
  updateStreamPopover(ctx);

  if(data.health) {

    applyHealthSnapshot(data.health, ctx);
  }
}

// streamAdded handler. Inserts the new stream into streamData and triggers a full table render plus popover refresh.
function handleStreamAdded(data: StreamSummary, ctx: HandlerContext): void {

  ctx.state.streamData[data.id] = data;
  renderStreamsTable(ctx);
  updateStreamPopover(ctx);
}

// streamRemoved handler. Drops the stream from streamData and expandedStreams, re-renders, and invokes the deferred-restart callback if config.ts has registered
// one. The optional ?.() form is used because the callback may not be registered yet when this handler fires.
function handleStreamRemoved(data: StreamRemovedPayload, ctx: HandlerContext): void {

  // The dynamic-delete lint rule fires because TS cannot statically verify the key exists. Here the key is a stream id that may or may not be present in the
  // record, so the delete is intentionally tolerant. eslint-disable on these two lines is the right call; the alternatives (Map<string, T>,
  // Record<string, T | undefined>) would either change the on-the-wire data shape or leave dead "undefined" entries that Object.values would still iterate.
  // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
  delete ctx.state.streamData[data.id];
  // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
  delete ctx.state.expandedStreams[data.id];
  renderStreamsTable(ctx);
  updateStreamPopover(ctx);
  ctx.externals.updateRestartDialogStatus?.();
}

// streamHealthChanged handler. Replaces the stream's record and chooses between a targeted row update (cheap, image-preserving) and a full table render
// (structural change). Also schedules a popover refresh so the duration/badge columns tick.
function handleStreamHealthChanged(data: StreamSummary, ctx: HandlerContext): void {

  const prev = ctx.state.streamData[data.id];

  if(!prev) {

    return;
  }

  const structuralChange = (prev.logoUrl !== data.logoUrl) || (prev.streamingMode !== data.streamingMode) ||
    (prev.hardwareAccelerated !== data.hardwareAccelerated) || (prev.captureCodec !== data.captureCodec);

  ctx.state.streamData[data.id] = data;

  if(structuralChange) {

    scheduleTableRender(ctx);
  } else {

    updateStreamRow(data, ctx);
  }

  schedulePopoverRender(ctx);
}

// systemStatusChanged handler. Replaces systemData and refreshes the header.
function handleSystemStatusChanged(data: SystemSummary, ctx: HandlerContext): void {

  ctx.state.systemData = data;
  updateSystemStatus(ctx);
}

// healthChanged handler. Targeted update for a single channel; if the result is success, also refreshes the domain-auth icons for that domain.
function handleHealthChanged(data: HealthChangedPayload, ctx: HandlerContext): void {

  updateChannelHealth(data.channelKey, data.status, data.timestamp, data.domain, ctx);

  if((data.status === "success") && data.domain) {

    updateDomainAuth(data.domain, data.timestamp, ctx);
  }
}

// channelUpdate handler. Forwards the patch to the shared channelTable namespace exposed by shared.ts.
function handleChannelUpdate(data: unknown, ctx: HandlerContext): void {

  ctx.externals.channelTable.applyPatch(data);
}

// onerror handler for the EventSource. Replaces the system-health badge with a stalled-color dot and "Updates paused" label so operators see the connection
// problem immediately.
function handleSseError(ctx: HandlerContext): void {

  const el = ctx.document.getElementById("system-health");

  if(el) {

    el.innerHTML = "<span class=\"status-dot\" style=\"color: var(--stream-stalled);\">&#9679;</span> Updates paused";
  }
}

// Window-bound trampolines and lifecycle helpers. These are invoked from the IIFE either via window.* assignment or directly.

// Toggle the stream popover open/closed. Wired to window.toggleStreamPopover by the IIFE so the header button's onclick can reach it.
function toggleStreamPopover(ctx: HandlerContext): void {

  const ids = Object.keys(ctx.state.streamData);

  if(ids.length === 0) {

    return;
  }

  const menu = ctx.document.getElementById("stream-popover-menu");

  if(!menu) {

    return;
  }

  const isOpen = menu.classList.contains("show");

  ctx.externals.dropdowns.close();

  if(!isOpen) {

    buildStreamPopoverContent(menu, ctx);
    menu.classList.add("show");
  }
}

// Copy the Quick Start playlist URL to the clipboard. Wired to window.copyOverviewPlaylistUrl by the IIFE so the Copy button's onclick can reach it.
function copyOverviewPlaylistUrl(ctx: HandlerContext): void {

  const urlEl = ctx.document.getElementById("overview-playlist-url");

  if(urlEl) {

    ctx.externals.copyToClipboard(urlEl.textContent, "Playlist URL copied to clipboard.");
  }
}

// visibilitychange handler. When the page returns from being hidden for more than 30 seconds, reconnect the SSE stream and re-activate the current tab so the
// logs stream reconnects naturally through its existing tabactivated listener. The reconnect callback is supplied by the IIFE - it owns the EventSource
// instance.
function handleVisibilityChange(ctx: HandlerContext, reconnect: () => void): void {

  if(ctx.document.hidden) {

    ctx.state.hiddenSince = Date.now();
  } else if((ctx.state.hiddenSince > 0) && ((Date.now() - ctx.state.hiddenSince) > 30000)) {

    ctx.state.hiddenSince = 0;
    reconnect();

    const activeTab = ctx.document.querySelector(".tab-btn.active");

    if(activeTab) {

      ctx.document.dispatchEvent(new CustomEvent("tabactivated", { detail: { category: activeTab.getAttribute("data-category") } }));
    }
  } else {

    ctx.state.hiddenSince = 0;
  }
}

// Initialize hover-emulating tooltips for devices whose primary input cannot hover (iPadOS Safari). A single shared tooltip element is positioned via
// getBoundingClientRect on hover and hidden on leave; immune to overflow containers and stacking contexts.
function initIPadTooltips(ctx: HandlerContext): void {

  if(!window.matchMedia("(hover: none)").matches) {

    return;
  }

  const tip = ctx.document.createElement("div");

  tip.className = "btn-icon-tooltip";
  ctx.document.body.appendChild(tip);
  ctx.document.addEventListener("mouseenter", (e: Event) => {

    const target = e.target as Element | null;
    const btn = target?.closest(".btn-icon[aria-label]");

    if(!btn) {

      return;
    }

    const label = btn.getAttribute("title") ?? btn.getAttribute("aria-label");

    if(!label) {

      return;
    }

    const rect = btn.getBoundingClientRect();

    tip.textContent = label;
    tip.classList.add("visible");
    tip.style.top = String(rect.bottom + 6 + (window.scrollY || 0)) + "px";
    tip.style.left = String(rect.left + (rect.width / 2) + (window.scrollX || 0)) + "px";
    tip.style.transform = "translateX(-50%)";
  }, true);
  ctx.document.addEventListener("mouseleave", (e: Event) => {

    const target = e.target as Element | null;
    const src = target?.closest(".btn-icon[aria-label]");

    if(!src) {

      return;
    }

    const related = (e as MouseEvent).relatedTarget as Element | null;
    const dest = related?.closest(".btn-icon[aria-label]");

    if(dest === src) {

      return;
    }

    tip.classList.remove("visible");
  }, true);
}

/**
 * The narrowly-typed function shape consumers of HANDLER_FUNCTIONS see. `(...args: never[]) => unknown` is the TS-idiomatic "any function" type that does not
 * fall back to the unsafe Function global - it is contravariant on parameters so it accepts any specific signature, and unknown on return so callers narrow at
 * the use site. The registry consumer (status.ts:generateStatusScript) only ever calls .toString() on each entry, so any function-shape works.
 */
type EmittableFn = (...args: never[]) => unknown;

/**
 * The script-side function registry. generateStatusScript() emits each entry's .toString() output in order. Order is hoisting-irrelevant for function
 * declarations but reflects the logical groups: pure formatters, state-derived renderers, DOM mutators, render schedulers, SSE handlers, then trampolines and
 * lifecycle helpers. Adding a new function means appending here and using its identifier in the IIFE; nothing else changes.
 *
 * The exported individual functions remain importable for tests; this registry is just the emission order for the script body.
 */
export const HANDLER_FUNCTIONS: readonly EmittableFn[] = [

  formatDuration,
  formatBytes,
  formatTime,
  formatTimeAgo,
  getDomain,
  getRecoveringLabel,
  getHealthBadge,
  formatLastIssue,
  formatAutoRecovery,
  formatClients,
  renderHealthCellContent,
  renderDetailCodec,
  renderDetailStarted,
  updateSystemStatus,
  buildStreamPopoverContent,
  updateStreamPopover,
  scheduleTableRender,
  schedulePopoverRender,
  renderStreamsTable,
  updateStreamRow,
  toggleStreamDetails,
  updateDurations,
  updateChannelHealth,
  updateDomainAuth,
  applyHealthSnapshot,
  handleSnapshot,
  handleStreamAdded,
  handleStreamRemoved,
  handleStreamHealthChanged,
  handleSystemStatusChanged,
  handleHealthChanged,
  handleChannelUpdate,
  handleSseError,
  toggleStreamPopover,
  copyOverviewPlaylistUrl,
  handleVisibilityChange,
  initIPadTooltips
];

// Test surface. Each function is exported so status.handlers.test.ts and the DOM-runtime suite at test/e2e/dom-runtime/status-handlers-runtime.test.ts can
// import and call them directly.
export {

  applyHealthSnapshot,
  buildStreamPopoverContent,
  copyOverviewPlaylistUrl,
  formatAutoRecovery,
  formatBytes,
  formatClients,
  formatDuration,
  formatLastIssue,
  formatTime,
  formatTimeAgo,
  getDomain,
  getHealthBadge,
  getRecoveringLabel,
  handleChannelUpdate,
  handleHealthChanged,
  handleSnapshot,
  handleSseError,
  handleStreamAdded,
  handleStreamHealthChanged,
  handleStreamRemoved,
  handleSystemStatusChanged,
  handleVisibilityChange,
  initIPadTooltips,
  renderDetailCodec,
  renderDetailStarted,
  renderHealthCellContent,
  renderStreamsTable,
  schedulePopoverRender,
  scheduleTableRender,
  toggleStreamDetails,
  toggleStreamPopover,
  updateChannelHealth,
  updateDomainAuth,
  updateDurations,
  updateStreamPopover,
  updateStreamRow,
  updateSystemStatus
};
