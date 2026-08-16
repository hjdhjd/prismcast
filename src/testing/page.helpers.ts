/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * page.helpers.ts: A general Puppeteer Page double for tests that drive page-shaped production code without a browser.
 */
import type { Page } from "puppeteer-core";

/* This is the general Page surface double: a stand-in for the handful of Page members that page-driving production code touches, with every asynchronous member
 * held open so the test decides when - and whether - it answers. Holding a call open is the point. A test that needs to observe what happens while an evaluate
 * is outstanding, or while a navigation is in flight, cannot get there with a double that answers immediately.
 *
 * The CDP-shaped double in cdp.helpers.ts is a separate thing: it stands in for a Page as the entry to a CDP session, where this one stands in for a Page as a
 * document to read and navigate.
 */

/**
 * One call the double received, held open for the test to settle. The timestamp is read at issue time, so a test driving mock timers can pin when production
 * code chose to make the call.
 */
export interface PendingPageCall<T> {

  // Value of Date.now() when the call was issued.
  readonly at: number;

  // Rejects the call with the supplied error.
  readonly reject: (error: unknown) => void;

  // Resolves the call with the supplied value.
  readonly resolve: (value: T) => void;
}

/**
 * Construction options for the Page double. Every handler receives the call as it is issued, along with its zero-based index in that member's call list; a
 * handler that settles the call decides the answer, and one that leaves it alone leaves the call pending for the test to settle later.
 */
export interface FakePageOptions {

  // What frames() reports. Defaults to an empty list.
  frames?: readonly unknown[];

  // Answers an evaluate call as it is issued.
  onEvaluate?: (call: PendingPageCall<unknown>, index: number) => void;

  // Answers a goto call as it is issued. Puppeteer's own goto resolves with a response or null, and the double mirrors that.
  onGoto?: (call: PendingPageCall<null>, index: number) => void;

  // Answers a waitForSelector call as it is issued.
  onWaitForSelector?: (call: PendingPageCall<unknown>, index: number) => void;

  // What browser().pages() resolves with. Defaults to an empty list.
  pages?: readonly unknown[];

  // What url() reports initially. Defaults to a placeholder test URL.
  url?: string;
}

/**
 * The double plus the handles a test drives it with.
 */
export interface FakePage {

  // Every evaluate call received, in issue order.
  readonly evaluations: PendingPageCall<unknown>[];

  // Every goto call received, in issue order.
  readonly navigations: PendingPageCall<null>[];

  // The Page-shaped double to hand to the code under test.
  readonly page: Page;

  // Every waitForSelector call received, in issue order.
  readonly selectorWaits: PendingPageCall<unknown>[];

  // Sets what isClosed() reports from here on.
  readonly setClosed: (closed: boolean) => void;

  // Sets what url() reports from here on.
  readonly setUrl: (url: string) => void;
}

/**
 * Records a call and hands back both the promise the double returns and the handle the test settles it with.
 * @returns The pending-call handle and the promise to hand back to the caller under test.
 */
function openCall<T>(): { call: PendingPageCall<T>; promise: Promise<T> } {

  const { promise, reject, resolve } = Promise.withResolvers<T>();

  return { call: { at: Date.now(), reject, resolve }, promise };
}

/**
 * Builds a Page double covering evaluate, isClosed, url, browser, frames, goto, and waitForSelector - the surface page-driving production code reaches for. The
 * cast through unknown is the established convention for these doubles: the double implements what the code under test calls and nothing else, so a structural
 * conformance to Puppeteer's full Page interface would be noise rather than safety.
 * @param options - Handlers and initial values for the double's members.
 * @returns The double and the handles for driving it.
 */
export function makeFakePage(options: FakePageOptions = {}): FakePage {

  const evaluations: PendingPageCall<unknown>[] = [];
  const navigations: PendingPageCall<null>[] = [];
  const selectorWaits: PendingPageCall<unknown>[] = [];

  let closed = false;
  let currentUrl = options.url ?? "https://page.helpers.test/";

  const page = {

    browser: (): unknown => ({ pages: async (): Promise<readonly unknown[]> => options.pages ?? [] }),
    evaluate: (): Promise<unknown> => {

      const { call, promise } = openCall<unknown>();

      evaluations.push(call);
      options.onEvaluate?.(call, evaluations.length - 1);

      return promise;
    },
    frames: (): readonly unknown[] => options.frames ?? [],
    goto: (): Promise<null> => {

      const { call, promise } = openCall<null>();

      navigations.push(call);
      options.onGoto?.(call, navigations.length - 1);

      return promise;
    },
    isClosed: (): boolean => closed,
    url: (): string => currentUrl,
    waitForSelector: (): Promise<unknown> => {

      const { call, promise } = openCall<unknown>();

      selectorWaits.push(call);
      options.onWaitForSelector?.(call, selectorWaits.length - 1);

      return promise;
    }
  } as unknown as Page;

  return {

    evaluations,
    navigations,
    page,
    selectorWaits,
    setClosed: (value: boolean): void => {

      closed = value;
    },
    setUrl: (value: string): void => {

      currentUrl = value;
    }
  };
}
