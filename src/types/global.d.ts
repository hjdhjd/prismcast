/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * global.d.ts: Global type declarations for PrismCast.
 */

declare global {

  // This is the video selector helper that is injected into the browser context via page.evaluateOnNewDocument. It is used by browser/video.ts to avoid
  // duplicating the video element selection pattern in every evaluateWithAbort call.
  interface Window {

    __prismcastSelectVideo?: (type: string) => HTMLVideoElement | null;
  }

  // Extend the NodeJS.Process interface to include the pkg property added by the pkg tool when running as a packaged executable.
  namespace NodeJS {

    interface Process {

      pkg?: {

        defaultEntrypoint: string;
        entrypoint: string;
        path: Record<string, string>;
      };
    }
  }
}

export {};
