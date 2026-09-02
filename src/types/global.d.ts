/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * global.d.ts: Global type declarations for PrismCast.
 */

declare global {

  /* These are the helpers PrismCast installs into the browser context. The video selector is injected via page.evaluateOnNewDocument and used by browser/video.ts to
   * avoid duplicating the video element selection pattern in every evaluateWithAbort call. The surface re-affirmation is a binding exposed by browser/index.ts on
   * every capture page, called by the focus listener that heals a capture the user has selected; it is optional because the listener can outlive an exposed binding
   * across a page's teardown.
   */
  interface Window {

    __prismcastReaffirmSurface?: () => Promise<void>;
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
