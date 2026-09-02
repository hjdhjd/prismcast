/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.captureLock.test.ts: Setup-tier test for createPageWithCapture's integration with the capture lock. It pins the closed-page recursion: when the page is found
 * already closed at the instant its capture turn is granted (a browser crash during the turn-wait), the lock task throws a typed PageClosedDuringTurnError, the caller
 * recurses on a fresh page outside the lock, and after MAX_PAGE_CLOSED_RETRIES it fails with the terminal message. The test drives createPageWithCapture through its
 * CreatePageWithCaptureDeps collaborators with a stub browser whose pages report isClosed() true, so getStream is never reached and no real Chrome runs. The wedge and
 * deadline mechanics are covered at the primitive tier in captureLock.test.ts; this test asserts only the setup-side wiring.
 */
import type { Browser, Page } from "puppeteer-core";
import { before, describe, test } from "node:test";
import { CONFIG } from "../config/index.ts";
import type { CreatePageWithCaptureDeps } from "./setup.ts";
import type { PuppeteerStream } from "puppeteer-stream";
import assert from "node:assert/strict";
import { createPageWithCapture } from "./setup.ts";
import { makeProfile } from "../config/profiles.helpers.ts";

// A minimal page that reports itself already closed. setBypassCSP and evaluateOnNewDocument are the only calls createPageWithCapture makes before the lock task, and
// the task's first statement is the isClosed() check, so a true here routes straight to the typed closed-page throw without ever reaching getStream.
function makeClosedStubPage(): Page {

  return {

    close: async (): Promise<void> => { /* The disposer never calls close because isClosed() is true, but a no-op keeps the stub total. */ },
    evaluateOnNewDocument: async (): Promise<void> => { /* The injected video-selector helper needs no real document on a stub. */ },
    isClosed: (): boolean => true,
    setBypassCSP: async (): Promise<void> => { /* Nothing to bypass on a stub. */ }
  } as unknown as Page;
}

// The injected browser-boundary collaborators. Each newPage hands back a fresh closed stub page, so every recursion sees a dead page. getStream must never be reached;
// it throws to make a regression that skipped the isClosed() check surface loudly.
const deps: CreatePageWithCaptureDeps = {

  emulateCaptureSurface: async (): Promise<{ height: number; width: number }> => ({ height: 1080, width: 1920 }),
  getCurrentBrowser: async (): Promise<Browser> => ({ newPage: async (): Promise<Page> => makeClosedStubPage() } as unknown as Browser),
  getStream: async (): Promise<PuppeteerStream> => { throw new Error("getStream must not run when the page is already closed at turn grant."); },
  startOverlayHandling: async (): Promise<void> => { /* No overlay poll runs on the closed-page path. */ },
  syncWindowVisibility: async (): Promise<void> => { /* Window presentation is not what this path measures. */ }
};

before(() => {

  // Native capture mode keeps the FFmpeg path (and its binary resolution) out of the setup that runs ahead of the lock task, so the closed-page throw is the only
  // outcome the call exercises.
  CONFIG.streaming.captureMode = "native";
});

describe("createPageWithCapture - closed-page turn recursion", () => {

  test("recurses on a fresh page and fails with the terminal message after the retry cap", async () => {

    const profile = makeProfile({ staticCapture: true });

    await assert.rejects(
      createPageWithCapture({ numericStreamId: 7, profile, skipManifestInterception: true, streamId: "closed-test", url: "https://closed.example/live" }, deps),
      (error: unknown) => (error instanceof Error) && (error.message === "Browser crashed too many times during capture initialization."),
      "the closed-page recursion exhausts the retry cap and throws the terminal error"
    );
  });
});
