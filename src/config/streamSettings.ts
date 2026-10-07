/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * streamSettings.ts: The per-stream settings snapshot for PrismCast.
 *
 * This module projects the configuration's next-stream settings into the value a stream reads for its whole life. Which leaves a stream copies is the
 * reactivity classification's projection, so it lives here beside the classification; when the copy is taken and who carries it is the stream's lifecycle, which
 * the streaming layer owns.
 */
import type { Config } from "../types/index.ts";

/**
 * The settings a stream reads for its whole life, copied from the running configuration once, when the stream's registry entry is created. Every setting
 * whose reactivity class is next-stream is a member, so a save reaches the streams that start after it while a running stream - its captures, its tab
 * replacements, its segmenters and its monitor - keeps the values it started with. The members are flat and declared here rather than derived from the
 * configuration's groups: no configuration group has this shape, so neither the running configuration nor a group of it can stand in for a snapshot, and a
 * read of a member resolves to this declaration rather than a configuration leaf, which is what lets the read-site census tell the two apart. Each member's
 * type is the leaf's own, by indexed access.
 */
export interface StreamSettings {

  // The audio bitrate, in bits per second, the stream's captures and its FFmpeg encoder run at.
  readonly audioBitsPerSecond: Config["streaming"]["audioBitsPerSecond"];

  // The frame rate the stream's capture bounds hold.
  readonly frameRate: Config["streaming"]["frameRate"];

  // The interval, in milliseconds, between the stream's health checks.
  readonly monitorInterval: Config["playback"]["monitorInterval"];

  // The duration, in seconds, the stream's segmenters cut at and declare as the playlist's target duration.
  readonly segmentDuration: Config["hls"]["segmentDuration"];

  // The video bitrate, in bits per second, the stream's captures encode at.
  readonly videoBitsPerSecond: Config["streaming"]["videoBitsPerSecond"];
}

/**
 * Copies a stream's settings out of a configuration. Production takes the copy exactly once per stream, when the stream registers, from the running
 * configuration. The copy is frozen because every holder of a stream shares the one object, and a write through any of them would move the values under the
 * stream's playlist, encoder and monitor alike.
 * @param config - The configuration to copy from, the running configuration in production.
 * @returns The stream's settings.
 */
export function snapshotStreamSettings(config: Config): StreamSettings {

  return Object.freeze({

    audioBitsPerSecond: config.streaming.audioBitsPerSecond,
    frameRate: config.streaming.frameRate,
    monitorInterval: config.playback.monitorInterval,
    segmentDuration: config.hls.segmentDuration,
    videoBitsPerSecond: config.streaming.videoBitsPerSecond
  });
}
