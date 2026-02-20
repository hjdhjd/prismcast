# Changelog

All notable changes to this project will be documented in this file.

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
