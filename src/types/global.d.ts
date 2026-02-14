/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * global.d.ts: Global type declarations for PrismCast.
 */

// Extend the NodeJS.Process interface to include the pkg property added by the pkg tool when running as a packaged executable.
declare namespace NodeJS {

  interface Process {

    pkg?: {

      defaultEntrypoint: string;
      entrypoint: string;
      path: Record<string, string>;
    };
  }
}
