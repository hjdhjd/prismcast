# Changelog

All notable changes to this project will be documented in this file.

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
