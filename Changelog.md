# Changelog

All notable changes to this project will be documented in this file.

## 1.10.0 (2026-05-10)
  * Improvement: Fox local affiliate channels are easier to set up and more reliable — PrismCast now detects your local Fox affiliate automatically on the first tune and remembers it for future tunes, so you don't have to look up your market's call sign yourself. Each tune is also verified to match the requested channel, and if you'd rather use a different local affiliate, you can edit the channel's selector to your preferred call sign.
  * Improvement: native HLS streaming now covers more providers — PrismCast can bypass screen capture for services that deliver media-only HLS playlists (no separate master manifest) by inferring codec details from the first segment, expanding the set of channels that stream at higher quality with lower CPU usage.
  * Improvement: HLS stream resilience for token-protected streams — when a provider's HLS manifest URL embeds an authentication token (in the path or as a query parameter) that expires mid-stream, PrismCast now refreshes the manifest with a fresh token instead of stalling on dead segment URLs, keeping native HLS streams alive across longer recordings.
  * Improvement: service management hardening — `install`, `start`, `stop`, `restart`, and `uninstall` commands are now fully asynchronous and no longer block the CLI during multi-second platform operations, and error messages surface the underlying tool's stderr text instead of a generic "Command failed" line. On Windows, the service launcher was rewritten to a single PowerShell script with stdout and stderr redirected to the data directory, eliminating a class of shell-quoting hazards in argument handling.
  * Improvement: persistence framework expansion — atomic writes with automatic backup recovery now apply across every configuration file (channels, profiles, health, and config), schema migrations are versioned and audited, and a cross-store consistency probe catches and repairs orphaned references at startup.
  * Improvement: configurable Channels DVR port — if your Channels DVR runs on a non-default port, you can now set it from the Advanced settings tab. PrismCast continues to auto-discover the DVR's host address; only the port is configurable.
  * Fix: profile saves applied in rapid succession now both apply correctly without one overwriting the other.
  * Fix: predictive pretuning now respects your service filter — channels you've hidden are skipped, keeping browser resources focused on your active lineup.
  * Fix: user-set channel numbers and station IDs on local-affiliate variants are now preserved across upgrades.
  * Fix: channel tags and guide titles containing quote or backslash characters no longer break the generated M3U playlist — attribute values are now properly escaped at every write site.
  * Housekeeping.

## 1.9.0 (2026-04-19)
  * New feature: M3U playlist tags and guide metadata — the playlist now includes `group-title` attributes from your channel tags, enabling automatic channel grouping in Channels DVR and other M3U consumers. Guide metadata (`tvg-id`, `tvg-name`, `tvg-logo`) is embedded for richer channel identification. Tags preserve the exact casing you entered.
  * New feature: informed channel creation — adding a custom channel now shows matching predefined channels as suggestions and warns when your active service filter would prevent the new channel from appearing.
  * Improvement: static page channels are now fully supported in the M3U playlist and HDHomeRun lineup.
  * Improvement: configuration persistence now uses atomic writes with automatic backup and recovery, so your settings are always safely saved.
  * Improvement: service selection indicators and channel override badges on the channels tab.
  * Improvement: provider terminology unified as service throughout the interface for a cleaner, more consistent experience.
  * Improvement: Windows service installation now uses a structured file-based task definition for more reliable installs and uninstalls.
  * Fix: Fox local affiliate service selection not persisting across restarts.
  * Housekeeping.

## 1.8.0 (2026-04-02)
  * New feature: channel tags — organize channels into groups like "sports", "news", "hbo", or "starz" for filtered playlists and channel management. Tags can be created, renamed, and deleted from the Manage Tags modal, assigned to channels via Quick Actions or inline editing, and used to filter the playlist with `?tag=` query parameters. Predefined channels ship with tags pre-assigned, including premium brand tags (HBO, Showtime, Starz) for subscription-based filtering. When the tag column filter is active, a playlist hint icon appears with the corresponding Channels DVR playlist URL ready to copy.
  * New feature: capture codec selection — control which codecs are eligible for browser capture. HEVC is used by default when GPU hardware encoding is available; H.264 is always enabled as the universal baseline. Users who experience issues with HEVC can disable it from the streaming settings.
  * Improvement: expanded predefined channel coverage.
  * Fix: Fox local affiliate channels defaulting to Cox instead of fox.com as the canonical provider.
  * Housekeeping.

## 1.7.0 (2026-03-27)
  * New feature: hardware-accelerated HEVC capture — when PrismCast detects that Chrome is using GPU-accelerated rendering, it automatically captures in HEVC/H.265 instead of H.264, delivering higher quality at lower bitrates with significantly reduced CPU usage. No configuration needed — detection and switching are fully automatic and seamless.
  * New feature: Cox Contour TV provider support with channel discovery. Thanks to @babsonnexus for the collaboration.
  * New feature: Browse Channels — a new wizard on the channels tab lets you discover and manage channels by provider. Select a provider, see all available channels with their current status (new, active, available via another provider), and add, switch, or remove channels in bulk. Channel logos are displayed using artwork from your Channels DVR library.
  * New feature: Provider Setup — a guided first-run wizard walks you through selecting your streaming providers, signing in, and building your initial channel lineup. Automatically appears on first visit and can be re-run anytime.
  * New feature: inline editing for channel numbers and station IDs — click any Number or Station ID cell in the channels table to edit it in place. Changes save on Enter or when you click away, and Escape cancels.
  * New feature: auto-number channels — assign sequential channel numbers to all visible channels based on the current sort order, or clear all channel numbers at once. Found in the Quick Actions menu.
  * New feature: per-channel HDHomeRun/Plex lineup control — choose which channels appear in the HDHomeRun lineup for Plex on a per-channel basis. A new opt-in HDHR column in the channels table provides inline checkboxes for quick toggling, and a bulk toggle in Quick Actions lets you include or exclude all channels at once. The add/edit channel form also includes the setting under Advanced Options. Channels excluded from the HDHR lineup remain available in the M3U playlist for Channels DVR.
  * Improvement: webUI improvements and refinements.
  * Improvement: resolution degradation detection and log message refinements.
  * Improvement: Hallmark site provider entries removed — Hallmark no longer offers direct streaming from their website. Hallmark, Hallmark Family, and Hallmark Mystery remain available through all TV provider variants (Cox, DirecTV, Hulu, Spectrum, Xfinity, YouTube TV).
  * Fix: provider filter not applied to predefined variant options in the channels tab dropdown.
  * Fix: user-set channel numbers on predefined channels now correctly appear when a non-default provider is selected.
  * Fix: filtered-out provider options in the provider dropdown no longer appear when a user customizes a predefined channel.
  * Housekeeping.

## 1.6.0 (2026-03-15)
  * New feature: Xfinity Stream provider support. Note: Xfinity's player is slow to initialize and tune — expect 15-30 seconds for channel changes. This is a limitation of the Xfinity Stream web player, not PrismCast. I'm exploring improvements for the future, but no promises — this is as good as it gets for now.
  * New feature: native HLS streaming — PrismCast automatically detects when a provider delivers non-DRMed HLS and bypasses screen capture entirely, consuming the stream directly for higher quality with lower CPU usage. Known to work with the A&E family (A&E, History, Lifetime), BET, C-SPAN, the Food Network family (Discovery, Food Network, HGTV, OWN, TLC, Travel, and others), Fox One, Fox Sports, VH1, and more. DRM-protected providers automatically fall back to screen capture.
  * New feature: preroll immediate response — HLS clients can receive video within seconds of a tune request rather than waiting for the full stream initialization to complete.
  * New feature: predictive channel pretuning — PrismCast reads the Channels DVR programming schedule and pretunes upcoming channels before recordings start, reducing tune latency to near zero.
  * New feature: dismiss intermittent site modals that block video playback.
  * New feature: video resolution degradation detection and recovery.
  * New feature: Docker Intel GPU hardware acceleration — containers with an Intel GPU can offload video processing from the CPU, significantly reducing CPU usage. Thanks to @ajvolin for the initial work and @bnhf for the contribution.
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
  * New feature: sortable columns and optional columns on the channels tab — click any column header to sort, and use the column picker to show or hide Number, Station ID, Profile, and Selector columns. Preferences persist across sessions. **Note: your preferred sort will determine how the playlist is ordered for Channels DVR by default.**
  * New feature: channel health and provider login indicators on the channels tab — green/red dots show last tune status per channel, and provider badges indicate verified authentication.
  * New feature: channel lineup precaching at startup — provider guide data can be optionally fetched in the background so channel discovery is instant on first tune. Precaching only helps speed up the first tune of a channel on a given provider.
  * New feature: bulk actions dropdown on the channels tab for toggling predefined channels by scope — all, Pacific variants only, or East variants only.
  * New feature: user-defined provider profiles — add support for any streaming site without waiting for a built-in update. A step-by-step builder wizard guides you through profile creation, live CSS selector testing verifies your configuration against the real site, and shareable provider packs let you export and import complete provider setups.
  * Improvement: the playlist endpoint now accepts optional `?sort=` and `?direction=` query parameters to override the saved sort order per request without changing the saved preference. Thanks to @bnhf for the inspiration.
  * Improvement: expanded predefined channel coverage across providers, with automatic Pacific timezone variant generation. Thanks to @bnhf for the collaboration.
  * Improvement: detect and fix stale service paths after upgrades — `service start` and `service restart` auto-regenerate the service file when paths change, and `service status` warns about mismatches.
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
  * New feature: Hulu direct tuning — channels now cache on first tune for faster subsequent tunes.
  * New feature: `upgrade` command for CLI and web UI — detects your install method (npm, Homebrew, Docker) and runs the appropriate upgrade.
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
