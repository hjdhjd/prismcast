/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * timing.ts: Timing measurement utilities for PrismCast.
 */

/**
 * Creates a lightweight elapsed-time closure using performance.now(). Call the returned function to get the elapsed milliseconds since creation.
 * @returns A closure that returns elapsed milliseconds as a number.
 */
export function startTimer(): () => number {

  const start = performance.now();

  return (): number => Math.round(performance.now() - start);
}
