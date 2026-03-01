/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * content.ts: Tab content HTML generators for the PrismCast landing page.
 */
import { escapeHtml, isRunningAsService } from "../../utils/index.js";
import { generateAdvancedTabContent, generateChannelsPanel, generateProvidersPanel, generateSettingsFormFooter, generateSettingsTabContent, generateWizardModal,
  hasEnvOverrides } from "../config/index.js";
import { getUITabs } from "../../config/userConfig.js";

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
 * Generates the Overview tab content with a comprehensive user guide covering what PrismCast is, video quality expectations, quick start instructions, tuning speed,
 * channel authentication, working with channels, and system requirements.
 * @param baseUrl - The base URL for the server.
 * @returns HTML content for the Overview tab.
 */
export function generateOverviewContent(baseUrl: string): string {

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
    "guide grid must be searched. Examples: DirecTV Stream, HBO Max, Hulu, Sling TV, YouTube TV, Fox.</p>",

    "<h4>Guide-Based Providers &mdash; Subsequent Tunes (~3&ndash;5 seconds)</h4>",
    "<p>After the first tune, PrismCast caches channel data for <strong>DirecTV Stream, HBO Max, Hulu, Sling TV, and YouTube TV</strong>. ",
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
export function generateHelpContent(): string {

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
export function generateApiReferenceContent(): string {

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
    "<tr>",
    "<td class=\"endpoint\"><code>POST /config/channels/display-prefs</code></td>",
    "<td>Update channel table display preferences: sort field, sort direction, and visible optional columns. " +
    "Body: <code>{ \"sortField\": \"name\", \"sortDirection\": \"asc\", \"visibleColumns\": [\"channelNumber\", \"stationId\"] }</code>. " +
    "All fields are optional. Persists to config file.</td>",
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
    "and optional <code>affiliate</code> and <code>tier</code> fields. Provider slugs: <code>directv</code>, <code>foxcom</code>, <code>hbomax</code>, " +
    "<code>hulu</code>, <code>sling</code>, <code>yttv</code>. Returns cached results instantly when a prior tune or discovery call has already " +
    "enumerated the lineup. " +
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
export function generateChannelsTabContent(): string {

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
export function generateLogsContent(): string {

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
 * Generates the Configuration tab content with subtabs for channels, settings, advanced, and backup.
 * @returns HTML content for the Configuration tab.
 */
export function generateConfigContent(): string {

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
