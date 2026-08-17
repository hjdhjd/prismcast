# Changelog

All notable changes to this project will be documented in this file.

## 1.11.0 (2026-08-16)
  * New feature: HDHomeRun LAN discovery - PrismCast now responds to standard HDHomeRun discovery broadcasts on UDP port 65001, so Plex finds PrismCast on the local network automatically without entering an address by hand. The new "Enable LAN Discovery" setting under HDHomeRun / Plex is on by default; turn it off in multi-tenant environments or when another real HDHomeRun device is already on the network. Channels DVR users continue to add PrismCast manually as a Custom Channels source - Channels DVR's HDHR auto-discovery assumes the standard HDHomeRun port 80, which most installations cannot bind.
  * New feature: automatic consent-prompt handling - sites that gate their player behind a cookie-consent or "enable tracking" prompt now tune on their own, so channels like France 24 work on a fresh setup. Prompts that can't be handled automatically point you to dismiss them once in login mode.
  * New feature: a per-channel "always use screen capture" option - for sites whose native HLS stream misbehaves, you can now force a channel to skip native streaming from the channel editor.
  * New feature: channel lineups now persist across restarts - PrismCast remembers each service's discovered channels and their direct watch addresses, so after a restart you can tune immediately even if the startup channel scan failed or hasn't run yet. A remembered address is always verified on use: if it has gone stale, the tune falls back to the guide in the same attempt and the lineup refreshes itself on the next successful scan.
  * Improvement: HDHomeRun emulation settings now apply live - turning emulation on or off, or changing the port, friendly name, or device ID, takes effect immediately without restarting the server. Settings outside HDHomeRun still require a restart, and the save confirmation tells you which changes applied live and which need one.
  * Improvement: PrismCast now recovers automatically when Chrome is connected but can no longer actually capture video - a state that used to produce silently broken streams until you restarted the server. It checks real capture readiness at every browser launch and relaunches a browser that has gone bad, with a loop-safe governor that backs off instead of thrashing when a browser keeps failing.
  * Improvement: a provider's guide page failing to render is now retried instead of accepted - the startup channel scan reloads and re-reads a service that returned no channels (and takes one more pass a few minutes later if it's still empty), and a tune that finds no guide reloads the page and retries channel selection once. Together these address the "zero channels precached, tuning failed until a restart" failure some services could hit on a slow boot.
  * Improvement: native HLS streaming is more robust over long recordings - encrypted streams now tolerate brief network hiccups on the audio track as patiently as on video, malformed decryption data is dropped instead of garbling the picture, channels with short-lived access tokens (some sports feeds) refresh cleanly at the token boundary instead of repeatedly reloading the page, and memory stays flat across very long sessions.
  * Improvement: the web interface is hardened against malformed or malicious channel data - logos, service URLs, show names, and stream URLs are consistently escaped and link schemes vetted, so untrusted text from a provider or imported service pack can't break the page or inject markup.
  * Improvement: when a Sling TV channel isn't found, the suggested-channels list in the log now leaves out channels a builtin preset already covers, matching the other providers.
  * Improvement: robustness work across the debugging surface, login-tab handling, and ad-hoc streaming, with no change to normal operation.
  * Fix: high-bitrate native HLS channels no longer degrade and fail after roughly an hour - PrismCast downloads several segments in parallel while serving them strictly in broadcast order, so delivery keeps pace with channels like Marquee Sports, and stopping a stream cancels its in-flight downloads immediately. A corrupted or truncated video fragment can no longer freeze the server in an endless parsing loop, and a failed audio-mute step can no longer take down a native stream.
  * Fix: channels that deliver modern fMP4/CMAF video over HLS now stream natively end to end - initialization data is carried through, segments are served under their true container type, and HDHomeRun/MPEG-TS clients receive a proper remux - so channels like Marquee Sports play correctly. Quality selection is also smarter about imperfect providers: when the top variant is broken, tuning falls back to the next-best feed instead of abandoning native streaming, the audio track is paired to the video variant actually selected, recordings no longer drift out of sync when a stream brings up a second audio or video track partway through capture, and encrypted channels whose playlists write the decryption IV in uppercase hex decrypt correctly.
  * Fix: native HLS streams now stay locked to the channel you tuned - the manifest PrismCast adopts is tied to the channel actually selected rather than whichever happened to arrive last, a token refresh hours into a recording re-tunes and verifies that same channel instead of silently drifting to the provider's default, and a refresh that hits transient trouble retries with backoff while binding the best feed the running stream can actually use, so token-protected channels survive refresh trouble without timers piling up.
  * Fix: services that put a static "channel bumper" in front of their live player (PBS Kids, for example) no longer produce a frozen stream that repeats one slate image - PrismCast now recognizes the bumper at tune time and serves the channel through screen capture immediately.
  * Fix: channel recovery is more reliable across providers - a stream that has to re-tune itself on Xfinity, Cox, DirecTV, or Hulu no longer stalls or falls back to a slow full guide reload; empty-guide recovery on Spectrum and YouTube TV recovers the one stream alone instead of clearing site data out from under a second active stream on the same service, releasing the browser resources it borrowed; and a resolution-degraded stream no longer gets stuck in a recovery loop after the page reloads.
  * Fix: Hulu channels whose guide name differs from the name you tune - local affiliates showing call signs, and punctuation variants like C-SPAN3 - now reuse their remembered guide position on repeat tunes instead of re-running the full 10-15 second guide search every time.
  * Fix: a browser tab that freezes mid-stream can no longer snowball into a dead recording - health checks now run one at a time instead of piling up against the frozen tab, where the pile-up could burn the entire recovery budget in an instant and terminate the stream while the real fix (replacing the hung tab) was still in flight. Hung tabs are still detected and replaced in about twenty seconds, and each recovery gets its full grace period, measured from when the recovery finished rather than when the health check began.
  * Fix: a channel's remembered direct watch URL now survives a tune that fails because the page or tab died mid-tune - only failures that say something about the URL itself evict it, so one browser hiccup no longer forces every later tune through full guide navigation, the path most likely to also be struggling during a provider incident.
  * Fix: nothing left over from an old stream can disturb a newer one on the same channel - a stream that is re-tuning no longer interferes with its replacement, a circuit-breaker verdict arriving after the channel was re-tuned terminates only the stream whose monitor reached it (never the replacement, and never twice), and stopping a stream responds immediately instead of waiting out a timeout.
  * Fix: starting two streams at the same instant could take the whole server down - capture startup is now serialized, so a failure affects only the stream that hit it. A stream request arriving while the old Chrome is still shutting down waits its turn instead of launching a second browser against the same profile, and a failed post-cooldown test launch re-enters the cooldown instead of resuming immediate relaunches.
  * Fix: stopping or restarting the server no longer leaves a stray Chrome process behind, and your latest channel health is saved on the way out. Slow-tuning channels can no longer have their browser tab closed out from under them mid-tune by the stale-page cleanup, and page tracking is fully reset at stream end and across browser restarts.
  * Fix: startup and shutdown are better behaved in bad conditions - when its port is already in use, PrismCast logs a clear message and exits instead of appearing to start while serving nothing, a shutdown signal during startup no longer wipes your channel health history, and a hung FFmpeg encode during startup degrades startup instead of hanging the server.
  * Fix: the in-app upgrade button now works - clicking "Start Upgrade" reported an error instead of upgrading on npm and Homebrew installs. Upgrading from the command line was unaffected.
  * Fix: web interface flow is smoother - the deferred-restart prompt now closes and restarts on its own once your streams end, the Channels DVR playlist reload hint appears for every change that affects the playlist (bulk operations and tag management included), and switching between the Config and Channels tabs no longer carries the other tab's subtab into the address bar, which could land a blank panel on back or forward navigation.
  * Fix: channel and configuration edits take effect predictably - configuration saves applied in rapid succession always leave the server running the newest saved state, editing a channel's URL, selector, or profile applies on the next tune instead of being masked for up to a day by a cached encryption probe, and editing a channel back to a variant's predefined values reverts it cleanly instead of leaving stale overrides behind.
  * Fix: profile editing is sturdier...a hand-edited profile that inherits from itself no longer hangs the settings page or the playlist, fields keep their exact text across editing round trips - a value containing an ampersand or markup-like text no longer changes each time the profile is reopened and saved - and imported settings and service packs are checked more carefully, rejecting values that don't match their expected type and stripping stray whitespace and invisible characters from text settings.
  * Fix: channel discovery is more dependable across Sling, HBO Max, DirecTV, Xfinity, Cox, and YouTube TV - a partial lineup read retries instead of being marked complete, removed channels drop from the cached lineup, a refreshed discovery no longer inherits stragglers from the walk it cancelled, and aborted discoveries settle promptly instead of polling out their clocks.
  * Fix: a channel that names a profile requiring a channel selector it doesn't define no longer has its choice silently replaced - a warning names the substitution at tune time.
  * Fix: HDHomeRun emulation plays nicer at the edges - the last available tuner is no longer wrongly rejected with an "all tuners in use" error when you're at the maximum number of concurrent streams, and an ampersand or angle bracket in your friendly name no longer breaks Plex's tuner discovery, so names like "Living Room & Office" work.
  * Fix: service management on Linux is more dependable - `prismcast service install --force` restarts a running service instead of leaving the old process in place, installing as a systemd service from a path containing a space generates a working service file, and the stale-path check reads both old and new service files.
  * Housekeeping.

## 1.10.3 (2026-05-17)
  * Improvement: native HLS streaming now applies to a much richer set of site types - if a site delivers unencrypted live HLS, PrismCast captures it natively, including local TV station live cams and pages with embedded third-party players, dramatically expanding the set of channels that stream at higher quality with lower CPU usage.
  * Improvement: channel health tooltip is now phrased as a full last-tune sentence for at-a-glance readability.
  * Fix: `prismcast upgrade` on Windows now runs the install step in a detached helper process, so installations where PrismCast runs as a service (or is set to auto-restart) complete cleanly instead of failing on locked files or stalling after the restart.
  * Fix: rare process-exit crash on Windows when the startup version check times out.
  * Fix: stale PID files no longer trigger "another PrismCast instance is already running" errors after reboots, crashes, or container restarts.
  * Housekeeping.

## 1.10.2 (2026-05-14)
  * Fix: HBO Max tuning updated for HBO's recent site restructure - live channels are now read from the new consolidated /channels hub instead of the old homepage menu navigation.
  * Improvement: Hulu's intermittent "Who's Watching?" profile selector is now dismissed automatically during tuning and channel discovery, so accounts with multiple profiles no longer get stuck on the picker overlay when Hulu re-prompts for profile selection across sessions.
  * Improvement: the default browser extension init timeout is now 3 seconds, giving slower machines more headroom before stream startup considers the capture pipeline failed.
  * Housekeeping.

## 1.10.1 (2026-05-10)
  * Housekeeping.

## 1.10.0 (2026-05-10)
  * Improvement: Fox local affiliate channels are easier to set up and more reliable - PrismCast now detects your local Fox affiliate automatically on the first tune and remembers it for future tunes, so you don't have to look up your market's call sign yourself. Each tune is also verified to match the requested channel, and if you'd rather use a different local affiliate, you can edit the channel's selector to your preferred call sign.
  * Improvement: native HLS streaming now covers more providers - PrismCast can bypass screen capture for services that deliver media-only HLS playlists (no separate master manifest) by inferring codec details from the first segment, expanding the set of channels that stream at higher quality with lower CPU usage.
  * Improvement: HLS stream resilience for token-protected streams - when a provider's HLS manifest URL embeds an authentication token (in the path or as a query parameter) that expires mid-stream, PrismCast now refreshes the manifest with a fresh token instead of stalling on dead segment URLs, keeping native HLS streams alive across longer recordings.
  * Improvement: service management hardening - `install`, `start`, `stop`, `restart`, and `uninstall` commands are now fully asynchronous and no longer block the CLI during multi-second platform operations, and error messages surface the underlying tool's stderr text instead of a generic "Command failed" line. On Windows, the service launcher was rewritten to a single PowerShell script with stdout and stderr redirected to the data directory, eliminating a class of shell-quoting hazards in argument handling.
  * Improvement: persistence framework expansion - atomic writes with automatic backup recovery now apply across every configuration file (channels, profiles, health, and config), schema migrations are versioned and audited, and a cross-store consistency probe catches and repairs orphaned references at startup.
  * Improvement: configurable Channels DVR port - if your Channels DVR runs on a non-default port, you can now set it from the Advanced settings tab. PrismCast continues to auto-discover the DVR's host address; only the port is configurable.
  * Fix: profile saves applied in rapid succession now both apply correctly without one overwriting the other.
  * Fix: predictive pretuning now respects your service filter - channels you've hidden are skipped, keeping browser resources focused on your active lineup.
  * Fix: user-set channel numbers and station IDs on local-affiliate variants are now preserved across upgrades.
  * Fix: channel tags and guide titles containing quote or backslash characters no longer break the generated M3U playlist - attribute values are now properly escaped at every write site.
  * Housekeeping.

## 1.9.0 (2026-04-19)
  * New feature: M3U playlist tags and guide metadata - the playlist now includes `group-title` attributes from your channel tags, enabling automatic channel grouping in Channels DVR and other M3U consumers. Guide metadata (`tvg-id`, `tvg-name`, `tvg-logo`) is embedded for richer channel identification. Tags preserve the exact casing you entered.
  * New feature: informed channel creation - adding a custom channel now shows matching predefined channels as suggestions and warns when your active service filter would prevent the new channel from appearing.
  * Improvement: static page channels are now fully supported in the M3U playlist and HDHomeRun lineup.
  * Improvement: configuration persistence now uses atomic writes with automatic backup and recovery, so your settings are always safely saved.
  * Improvement: service selection indicators and channel override badges on the channels tab.
  * Improvement: provider terminology unified as service throughout the interface for a cleaner, more consistent experience.
  * Improvement: Windows service installation now uses a structured file-based task definition for more reliable installs and uninstalls.
  * Fix: Fox local affiliate service selection not persisting across restarts.
  * Housekeeping.

## 1.8.0 (2026-04-02)
  * New feature: channel tags - organize channels into groups like "sports", "news", "hbo", or "starz" for filtered playlists and channel management. Tags can be created, renamed, and deleted from the Manage Tags modal, assigned to channels via Quick Actions or inline editing, and used to filter the playlist with `?tag=` query parameters. Predefined channels ship with tags pre-assigned, including premium brand tags (HBO, Showtime, Starz) for subscription-based filtering. When the tag column filter is active, a playlist hint icon appears with the corresponding Channels DVR playlist URL ready to copy.
  * New feature: capture codec selection - control which codecs are eligible for browser capture. HEVC is used by default when GPU hardware encoding is available; H.264 is always enabled as the universal baseline. Users who experience issues with HEVC can disable it from the streaming settings.
  * Improvement: expanded predefined channel coverage.
  * Fix: Fox local affiliate channels defaulting to Cox instead of fox.com as the canonical provider.
  * Housekeeping.

## 1.7.0 (2026-03-27)
  * New feature: hardware-accelerated HEVC capture - when PrismCast detects that Chrome is using GPU-accelerated rendering, it automatically captures in HEVC/H.265 instead of H.264, delivering higher quality at lower bitrates with significantly reduced CPU usage. No configuration needed - detection and switching are fully automatic and seamless.
  * New feature: Cox Contour TV provider support with channel discovery. Thanks to @babsonnexus for the collaboration.
  * New feature: Browse Channels - a new wizard on the channels tab lets you discover and manage channels by provider. Select a provider, see all available channels with their current status (new, active, available via another provider), and add, switch, or remove channels in bulk. Channel logos are displayed using artwork from your Channels DVR library.
  * New feature: Provider Setup - a guided first-run wizard walks you through selecting your streaming providers, signing in, and building your initial channel lineup. Automatically appears on first visit and can be re-run anytime.
  * New feature: inline editing for channel numbers and station IDs - click any Number or Station ID cell in the channels table to edit it in place. Changes save on Enter or when you click away, and Escape cancels.
  * New feature: auto-number channels - assign sequential channel numbers to all visible channels based on the current sort order, or clear all channel numbers at once. Found in the Quick Actions menu.
  * New feature: per-channel HDHomeRun/Plex lineup control - choose which channels appear in the HDHomeRun lineup for Plex on a per-channel basis. A new opt-in HDHR column in the channels table provides inline checkboxes for quick toggling, and a bulk toggle in Quick Actions lets you include or exclude all channels at once. The add/edit channel form also includes the setting under Advanced Options. Channels excluded from the HDHR lineup remain available in the M3U playlist for Channels DVR.
  * Improvement: webUI improvements and refinements.
  * Improvement: resolution degradation detection and log message refinements.
  * Improvement: Hallmark site provider entries removed - Hallmark no longer offers direct streaming from their website. Hallmark, Hallmark Family, and Hallmark Mystery remain available through all TV provider variants (Cox, DirecTV, Hulu, Spectrum, Xfinity, YouTube TV).
  * Fix: provider filter not applied to predefined variant options in the channels tab dropdown.
  * Fix: user-set channel numbers on predefined channels now correctly appear when a non-default provider is selected.
  * Fix: filtered-out provider options in the provider dropdown no longer appear when a user customizes a predefined channel.
  * Housekeeping.

## 1.6.0 (2026-03-15)
  * New feature: Xfinity Stream provider support. Note: Xfinity's player is slow to initialize and tune - expect 15-30 seconds for channel changes. This is a limitation of the Xfinity Stream web player, not PrismCast. I'm exploring improvements for the future, but no promises - this is as good as it gets for now.
  * New feature: native HLS streaming - PrismCast automatically detects when a provider delivers non-DRMed HLS and bypasses screen capture entirely, consuming the stream directly for higher quality with lower CPU usage. Known to work with the A&E family (A&E, History, Lifetime), BET, C-SPAN, the Food Network family (Discovery, Food Network, HGTV, OWN, TLC, Travel, and others), Fox One, Fox Sports, VH1, and more. DRM-protected providers automatically fall back to screen capture.
  * New feature: preroll immediate response - HLS clients can receive video within seconds of a tune request rather than waiting for the full stream initialization to complete.
  * New feature: predictive channel pretuning - PrismCast reads the Channels DVR programming schedule and pretunes upcoming channels before recordings start, reducing tune latency to near zero.
  * New feature: dismiss intermittent site modals that block video playback.
  * New feature: video resolution degradation detection and recovery.
  * New feature: Docker Intel GPU hardware acceleration - containers with an Intel GPU can offload video processing from the CPU, significantly reducing CPU usage. Thanks to @ajvolin for the initial work and @bnhf for the contribution.
  * Improvement: native proxy upstream metadata propagation for HLS discontinuity, SCTE-35 cues, and program date-time.
  * Improvement: track-aware segment health monitoring with provider-specific thresholds.
  * Improvement: video readiness enhancements with per-domain timeout, offscreen scrolling, and diagnostic logging.
  * Improvement: increased granularity of login indicators.
  * Improvement: cross-platform Chrome process cleanup via PID file instead of pkill/pgrep.
  * Improvement: Sling TV precache resilience for slow connections.
  * Improvement: additional Sling TV channel definitions.
  * Improvement: Hulu local affiliate tuning skips the guide grid when precaching is enabled, reducing first-tune latency.
  * Fix: channels tab provider dropdown now correctly reflects the provider filter instead of showing filtered-out providers.
  * Fix: C-SPAN tuning failures caused by preroll ads and offscreen video.
  * Fix: display detection on minimized Chrome windows and tab replacement compositor stability.
  * Fix: prevent terminated streams from persisting in the dashboard.
  * Fix: decrement resume segment index to prevent Channels DVR from dropping the last completed segment.
  * Housekeeping: prevent multiple server instances from running simultaneously.
  * Housekeeping: shutdown resiliency improvements.
  * Housekeeping.

## 1.5.2 (2026-03-01)
  * Improvement: expanded Spectrum TV predefined channel coverage.
  * Housekeeping.

## 1.5.1 (2026-03-01)
  * New feature: Spectrum TV provider support inclusive of local affiliates. Thanks to @scottuf for the collaboration.
  * Improvement: added 40+ DirecTV Stream (thanks to @mackid1993) and Sling channel variants (thanks to @bnhf), including local affiliate support for DirecTV Stream.
  * Improvement: webUI refinements - consolidated Quick Actions menu with live toggle counts for predefined channel scopes, bulk provider assignment, and a channel summary showing predefined and user channel breakdown.
  * Housekeeping.

## 1.5.0 (2026-02-28)
  * New feature: DirecTV Stream provider support. Thanks to @kineticmac for the collaboration.
  * New feature: sortable columns and optional columns on the channels tab - click any column header to sort, and use the column picker to show or hide Number, Station ID, Profile, and Selector columns. Preferences persist across sessions. **Note: your preferred sort will determine how the playlist is ordered for Channels DVR by default.**
  * New feature: channel health and provider login indicators on the channels tab - green/red dots show last tune status per channel, and provider badges indicate verified authentication.
  * New feature: channel lineup precaching at startup - provider guide data can be optionally fetched in the background so channel discovery is instant on first tune. Precaching only helps speed up the first tune of a channel on a given provider.
  * New feature: bulk actions dropdown on the channels tab for toggling predefined channels by scope - all, Pacific variants only, or East variants only.
  * New feature: user-defined provider profiles - add support for any streaming site without waiting for a builtin update. A step-by-step builder wizard guides you through profile creation, live CSS selector testing verifies your configuration against the real site, and shareable provider packs let you export and import complete provider setups.
  * Improvement: the playlist endpoint now accepts optional `?sort=` and `?direction=` query parameters to override the saved sort order per request without changing the saved preference. Thanks to @bnhf for the inspiration.
  * Improvement: expanded predefined channel coverage across providers, with automatic Pacific timezone variant generation. Thanks to @bnhf for the collaboration.
  * Improvement: detect and fix stale service paths after upgrades - `service start` and `service restart` auto-regenerate the service file when paths change, and `service status` warns about mismatches.
  * Improvement: profile-level scroll options for sites like Disney+ that lazy-load page content.
  * Improvement: webUI performance refinements.
  * Improvement: channel selector autocomplete now suggests all available channels from provider discovery, with fuzzy URL matching and "Did you mean?" hints for common domain variants.
  * Housekeeping: provider optimizations and refinements.
  * Housekeeping.

## 1.4.2 (2026-02-21)
  * Improvement: Hulu tuning refinements.
  * Improvement: channels tab modernization and refinements.
  * Housekeeping: reorganized API Reference with navigation index and expanded endpoint documentation.
  * Housekeeping: accessibility improvements.

## 1.4.1 (2026-02-20)
  * Improvement: webUI refinements.
  * Fix: static page profiles (e.g., Weatherscan) no longer trigger false recovery loops from the playback monitor searching for a nonexistent video element.
  * Fix: detect stale capture pipelines that stop producing segments entirely, even when the video element appears healthy.
  * Housekeeping.

## 1.4.0 (2026-02-19)
  * New feature: Hulu direct tuning - channels now cache on first tune for faster subsequent tunes.
  * New feature: `upgrade` command for CLI and web UI - detects your install method (npm, Homebrew, Docker) and runs the appropriate upgrade.
  * New feature: optionally include channel numbers in the M3U playlist for Channels DVR.
  * New feature: configurable data, Chrome profile, and log file paths via `--data-dir`, `--chrome-data-dir`, and `--log-file` CLI flags and environment variables, with a new `--list-env` option to list all available settings.
  * Improvement: smoother stream continuity and resiliency across playback recovery boundaries.
  * Improvement: cleaner audio transitions when switching between channels.
  * Improvement: more flexible site profile system for channel matching, fullscreen handling, and overlay suppression.
  * Improvement: broader compatibility with additional streaming site layouts.
  * Improvement: Docker entrypoint supports custom data, Chrome, and log directories via environment variables for flexible volume mount configurations.
  * Improvement: additions and refinements to predefined channels.
  * Fix: provider playlist filter now correctly honors user-specified include and exclude selections.
  * Housekeeping.

## 1.3.4 (2026-02-16)
  * Improvement: documentation updates.
  * Improvement: give Chrome additional time to shutdown gracefully to prevent profile database corruption in Docker volumes.
  * Housekeeping.

## 1.3.3 (2026-02-16)
  * Improvement: the playlist endpoint now supports multi-provider and exclude filters (e.g., `?provider=yttv,sling` or `?provider=-hulu`) with input validation.
  * Improvement: refreshed the PrismCast server home page documentation.
  * Improvement: defensively clean up after Chrome on startup and shutdown.
  * Housekeeping.

## 1.3.2 (2026-02-15)
  * Improvement: when possible, directly tune URLs for HBO Max, Sling TV, and YouTube TV to skip guide navigation on repeat tunes.
  * Improvement: our stream health monitoring now regularly checks to ensure the stream remains fullscreened, and attempts to correct it if it's not.
  * Improvement: improved MPEG-TS ATSC transport stream compatibility for Plex HDHomeRun integration.
  * Improvement: webUI refinements.
  * Fix: saving settings was wiping the disabled channel list, provider filter, and HDHomeRun device ID.
  * Housekeeping.

## 1.3.1 (2026-02-14)
  * Improvement: when channel selection fails, logs available channel names from the provider's guide to help users identify the correct channel selector value for user-defined channels.
  * Improvement: YouTube TV channel matching now handles parenthetical suffix variants and additional PBS affiliate names.
  * Fix: channel selection failures now abort the stream instead of silently serving the wrong channel.
  * Fix: web UI regression.
  * Housekeeping.

## 1.3.0 (2026-02-14)
  * New feature: Fox.com provider support.
  * New feature: Sling TV provider support with automatic local affiliate resolution for broadcast networks.
  * New feature: provider filtering. Choose which subscription services are active in your environment and filter channels accordingly.
  * Improvement: streaming startup and playback recovery performance optimizations.
  * Improvement: stream resiliency and recovery improvements.
  * Improvement: additions and refinements to predefined channels.
  * Improvement: UI refinements.
  * Housekeeping.

## 1.2.1 (2026-02-08)
  * New feature: HBO Max provider support.
  * New feature: YouTube TV provider support with automatic local affiliate resolution for broadcast networks and PBS.
  * New feature: proactive page reload for sites with continuous playback limits (e.g., NBC.com).
  * Fix: false positive dead capture detection on lower quality presets causing continuous tab replacement loops.
  * Housekeeping.

## 1.2.0 (2026-02-07)
  * New feature: Homebrew tap for macOS installation (`brew install hjdhjd/prismcast/prismcast`). Upgrade it like any Homebrew package after that.
  * New feature: Automated Docker builds based on the contributions of @bnhf. Latest official release can always be installed from: `docker pull ghcr.io/hjdhjd/prismcast:latest`.
  * New feature: Hulu support.
  * Improvement: DisneyNOW, Hulu, Sling, and additional channels and providers added.
  * Improvement: The channels tab has been rethought to handle multiple provider types. Now you can decide which provider you'd like to use for which channel, or override them all with a user-defined channel if you prefer. **Note: I would strongly encourage users to embrace the defaults and not create user-defined channels unless they are necessary in your environment. The predefined channels represent what is tested and will be maintained. If you've defined channels previously that are now built into PrismCast, I would encourage you to streamline your environment and delete the user-defined channel and use the appropriate builtin version. You don't have to do this...but it will make your quality of life better as PrismCast evolves and your user-defined channels don't keep up with PrismCast's updates.**
  * Improvement: UI refinements.
  * Behavior change: native capture mode is now disabled due to a Chrome bug that produces corrupt output after a few minutes. Hopefully Chrome addresses this in the future and I can make this available again.
  * Housekeeping.

## 1.1.0 (2026-02-03)
  * New feature: ad-hoc URL streaming via `/play` endpoint. Stream any URL without creating a channel definition.
  * New feature: Docker and LXC container support with prebuilt images, VNC/noVNC access, and Docker Compose configuration, courtesy of @bnhf.
  * Improvement: streaming startup performance optimizations.
  * Improvement: channel profile additions and refinements.
  * Improvement: webUI improvements.
  * Housekeeping.

## 1.0.12 (2026-02-01)
  * New feature: HDHomeRun emulation for Plex integration. PrismCast can now appear as a virtual HDHomeRun tuner, allowing Plex to discover and record channels directly.
  * New feature: predefined channel enable/disable controls with bulk toggle.
  * Improvement: streamlined channels tab with consolidated toolbar, import dropdown, and channel selector suggestions for known multi-channel sites.
  * Improvement: additions and refinements to predefined channels and site audodetection presets.
  * Improvement: additions and refinements to the PrismCast API.
  * Improvement: refinements to the active streams panel.
  * Improvement: smoother stream recovery with HLS discontinuity markers.
  * Housekeeping.

## 1.0.11 (2026-01-27)
  * Housekeeping.

## 1.0.10 (2026-01-26)
  * Housekeeping.

## 1.0.9 (2026-01-26)
  * Housekeeping.

## 1.0.8 (2026-01-25)
  * Improvement: version display refinements.
  * Housekeeping.

## 1.0.7 (2026-01-25)
  * New feature: version display in header with update checking and changelog modal.
  * Improvement: startup and shutdown robustness.
  * Fix: channel duplication when creating override channels.
  * Fix: double punctuation in error log messages.
  * Fix: active streams table spacing.
  * Housekeeping.

## 1.0.6 (2026-01-25)
  * New feature: display channel logos from Channels DVR in the active streams panel.
  * New feature: profile reference documentation UI with summaries in the dropdown.
  * Improvement: active streams panel styling and font consistency.
  * Improvement: graceful shutdown handling.
  * Fix: monitor status emit race conditions and duplicate emits.

## 1.0.5 (2026-01-24)
  * Housekeeping.

## 1.0.4 (2026-01-24)
  * Housekeeping.

## 1.0.3 (2026-01-24)
  * Housekeeping.

## 1.0.2 (2026-01-24)
  * Fix stale SSE status updates after tab reload.
  * Housekeeping.

## 1.0.1 (2026-01-24)
  * Housekeeping.

## 1.0.0 (2026-01-24)
  * Initial release.
