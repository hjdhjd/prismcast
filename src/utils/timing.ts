/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * timing.ts: Timing measurement utilities for PrismCast. Reads the clock through the Clock port (see clock.ts) so tests can inject a deterministic time source;
 * production callers omit the argument and the default-argument wires through to realClock which delegates to performance.now().
 */
import type { Clock } from "./clock.ts";
import { realClock } from "./clock.ts";

/**
 * Creates a lightweight elapsed-time closure. Captures the clock's current reading at creation and rounds the elapsed delta on each call. Production callers
 * pass no argument and consume the realClock default; tests inject a fake clock for deterministic time control.
 * @param clock - The clock to read time from. Defaults to realClock (which wraps performance.now()).
 * @returns A closure that returns elapsed milliseconds as a rounded integer.
 */
export function startTimer(clock: Clock = realClock): () => number {

  const start = clock.now();

  return (): number => Math.round(clock.now() - start);
}
