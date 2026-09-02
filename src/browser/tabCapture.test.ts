/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tabCapture.test.ts: Unit tests for PrismCast's own capture acquisition. The module speaks the capture extension's protocol directly, so what is pinned here is
 * that protocol as a sequence of observable acts: a socket handler registered before the extension is invoked, one bringToFront ahead of the activeTab keyboard
 * command, the tab query between that command and the start, and a settings object whose fields are exactly the ones the extension destructures.
 *
 * Everything is faked in this file rather than mocked at the loader, following the convention the bespoke page doubles in browser/tuning use: an EventEmitter
 * standing in for the WebSocket server, an emitter-backed socket, an extension page whose evaluate dispatches on the source text of the callback it is handed, and
 * a page that records its keyboard traffic. Two rows reach outside those fakes on purpose - the fingerprint and settings-shape pins read the installed library
 * through import.meta.resolve, because the coupling this module takes on is only safe while the source it was written against is the source that is installed.
 */
import { ACTIVE_TAB_GRANT_CEILING_MS, ACTIVE_TAB_GRANT_POLL_MS, CAPTURE_FRAME_SIZE_MS, CAPTURE_SOURCE_UNAVAILABLE_MESSAGE, CAPTURE_START_ATTEMPTS,
  CAPTURE_STREAM_HIGH_WATER_MARK, EXTENSION_NOT_READY_MESSAGE, EXTENSION_READY_EXPRESSION, NO_ACTIVE_TAB_MESSAGE, acquireCaptureStream } from "./tabCapture.ts";
import type { Browser, Page } from "puppeteer-core";
import type { CaptureStreamOptions, ExtensionRecordingSettings, TabCaptureDeps } from "./tabCapture.ts";
import type { WebSocket, WebSocketServer } from "ws";
import { describe, test } from "node:test";
import { makeAdvancingClock, makeFakeClock } from "../utils/clock.helpers.ts";
import { EventEmitter } from "node:events";
import type { LogEntry } from "../utils/logEmitter.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { readFile } from "node:fs/promises";
import { subscribeToLogs } from "../utils/logEmitter.ts";

// This file imports the module that imports puppeteer-stream, which spawns a WebSocketServer at load and would otherwise hold the runner open.
closePuppeteerStreamWssOnIdle();

// The capture options every row asks for, unless it needs a different shape. The constraint numbers are incidental here - what matters is that they arrive at the
// extension unchanged.
const OPTIONS: CaptureStreamOptions = {

  audio: true,
  audioBitsPerSecond: 128000,
  mimeType: "video/mp4;codecs=avc1,mp4a.40.2",
  video: true,
  videoBitsPerSecond: 8000000,
  videoConstraints: {

    mandatory: { maxFrameRate: 60, maxHeight: 1080, maxWidth: 1920, minFrameRate: 30, minHeight: 1080, minWidth: 1920 }
  }
};

// One evaluate the extension page served, tagged by which of the four protocol calls it was.
interface ExtensionCall {

  arg: unknown;
  kind: "ready" | "start" | "stop" | "tabs";
}

// The socket double: an emitter the test drives, plus the flags the production code reads or sets.
interface FakeSocket {

  closeCalls: number;
  emitClose: () => void;
  emitMessage: (data: string) => void;
  socket: WebSocket;
}

// The server double: a real EventEmitter underneath, so registration, removal, and listener counting behave exactly as the ws server's do.
interface FakeServer {

  emitConnection: (socket: FakeSocket, index: number) => void;
  listeners: () => number;
  server: WebSocketServer;
}

// The extension page double, with the record of every protocol call it served.
interface FakeExtension {

  calls: ExtensionCall[];
  disconnect: () => void;
  page: Page;
}

// The capture page double, with the keyboard and front-bringing traffic it recorded and the close handler it was given.
interface FakePage {

  emitClose: () => void;
  page: Page;
}

/**
 * Builds the socket double.
 * @param timeline - The shared ordering record.
 * @returns The socket double.
 */
function makeFakeSocket(timeline: string[]): FakeSocket {

  const emitter = new EventEmitter();

  const handle: FakeSocket = {

    closeCalls: 0,
    emitClose: (): void => { emitter.emit("close"); },
    emitMessage: (data: string): void => { emitter.emit("message", Buffer.from(data)); },
    socket: {

      close: (): void => {

        handle.closeCalls++;
        timeline.push("socket:close");
      },
      on: (event: string, listener: (...args: unknown[]) => void): unknown => emitter.on(event, listener)
    } as unknown as WebSocket
  };

  return handle;
}

/**
 * Builds the server double over a real EventEmitter, so listener registration, removal, and counting are the genuine article rather than a tally.
 * @param timeline - The shared ordering record.
 * @returns The server double.
 */
function makeFakeServer(timeline: string[]): FakeServer {

  const emitter = new EventEmitter();

  const server = {

    listenerCount: (event: string): number => emitter.listenerCount(event),
    off: (event: string, listener: (...args: unknown[]) => void): unknown => {

      timeline.push("handler:off");

      return emitter.off(event, listener);
    },
    on: (event: string, listener: (...args: unknown[]) => void): unknown => {

      timeline.push("handler:on");

      return emitter.on(event, listener);
    }
  } as unknown as WebSocketServer;

  return {

    emitConnection: (socket: FakeSocket, index: number): void => {

      emitter.emit("connection", socket.socket, { url: "/?index=" + String(index) });
    },
    listeners: (): number => emitter.listenerCount("connection"),
    server
  };
}

/**
 * Builds the extension-page double. Its evaluate dispatches on the source text of what it is handed, which is how one double serves all four protocol calls
 * without the production code needing a test-only branch.
 * @param timeline - The shared ordering record.
 * @param options - The scripted answers.
 * @param options.onStart - What START_RECORDING does, given the settings and the one-based attempt number. Defaults to resolving.
 * @param options.ready - What the readiness expression answers. Defaults to true.
 * @param options.tabs - What the tab query answers. Defaults to one tab.
 * @returns The extension double.
 */
function makeFakeExtension(timeline: string[], options: { onStart?: (settings: ExtensionRecordingSettings, attempt: number) => Promise<void>; ready?: boolean;
  tabs?: { id?: number; url?: string }[]; } = {}): FakeExtension {

  const calls: ExtensionCall[] = [];

  let connected = true;
  let starts = 0;

  const evaluate = async (target: unknown, arg?: unknown): Promise<unknown> => {

    const source = String(target);

    if(source === EXTENSION_READY_EXPRESSION) {

      calls.push({ arg, kind: "ready" });
      timeline.push("ready");

      return options.ready ?? true;
    }

    if(source.includes("START_RECORDING")) {

      starts++;
      calls.push({ arg, kind: "start" });
      timeline.push("start");

      return options.onStart?.(arg as ExtensionRecordingSettings, starts);
    }

    if(source.includes("STOP_RECORDING")) {

      calls.push({ arg, kind: "stop" });
      timeline.push("stop");

      return undefined;
    }

    if(source.includes("chrome.tabs.query")) {

      calls.push({ arg, kind: "tabs" });
      timeline.push("tabs");

      return options.tabs ?? [{ id: 42, url: "https://example.test/live" }];
    }

    throw new Error("Unserved evaluate shape: " + source.slice(0, 160));
  };

  return {

    calls,
    disconnect: (): void => { connected = false; },
    page: {

      browser: (): Browser => ({ connected } as unknown as Browser),
      evaluate,
      isClosed: (): boolean => false
    } as unknown as Page
  };
}

/**
 * Builds the capture-page double, recording the front-bringing and keyboard traffic the activeTab grant is made of.
 * @param timeline - The shared ordering record.
 * @returns The page double.
 */
function makeFakePage(timeline: string[]): FakePage {

  const emitter = new EventEmitter();

  return {

    emitClose: (): void => { emitter.emit("close"); },
    page: {

      bringToFront: async (): Promise<void> => { timeline.push("bringToFront"); },
      browser: (): Browser => ({ connected: true } as unknown as Browser),

      /* The retry's diagnostic reads the window's state through this page, so the double answers the two commands that read takes. Both answers are the ordinary
       * ones: a resolvable window that reports itself presented.
       */
      createCDPSession: async (): Promise<unknown> => ({

        send: async (method: string): Promise<unknown> => {

          if(method === "Browser.getWindowForTarget") {

            return { windowId: 7 };
          }

          return { bounds: { windowState: "normal" } };
        }
      }),
      isClosed: (): boolean => false,
      keyboard: {

        down: async (key: string): Promise<void> => { timeline.push("down:" + key); },
        press: async (key: string): Promise<void> => { timeline.push("press:" + key); },
        up: async (key: string): Promise<void> => { timeline.push("up:" + key); }
      },
      once: (event: string, listener: () => void): unknown => emitter.once(event, listener)
    } as unknown as Page
  };
}

/**
 * Runs a body with every emitted log entry captured, and hands back the warnings among them.
 * @param body - The work to run under capture.
 * @returns The warn-level entries emitted while the body ran.
 */
async function captureWarnings(body: () => Promise<void>): Promise<LogEntry[]> {

  const captured: LogEntry[] = [];
  const unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });

  try {

    await body();
  } finally {

    unsubscribe();
  }

  return captured.filter((entry) => entry.level === "warn");
}

/**
 * Composes the deps an acquisition runs against.
 * @param extension - The extension double.
 * @param server - The server double.
 * @returns The deps.
 */
function makeDeps(extension: FakeExtension, server: FakeServer): TabCaptureDeps {

  // The two resolutions of the ws types (this file's and the library declaration's) are structurally the same server with different nominal identities, so the
  // fake is handed over through the same cast every page double in the suite uses.
  return { getExtensionPage: async (): Promise<Page> => extension.page, wss: Promise.resolve(server.server) as unknown as TabCaptureDeps["wss"] };
}

describe("acquireCaptureStream", () => {

  test("registers the socket handler before invoking the extension, and grants activeTab before asking which tab is active", async () => {

    /* The order is the protocol, not a preference. START_RECORDING opens its socket before it asks Chrome for the capture, so a handler registered afterwards
     * misses the connection outright. The keyboard command is what grants the extension activeTab for the page in front, so it has to follow bringToFront and
     * precede the query whose answer the start is aimed at.
     */
    const timeline: string[] = [];
    const server = makeFakeServer(timeline);
    const extension = makeFakeExtension(timeline);
    const page = makeFakePage(timeline);
    const { clock } = makeFakeClock();

    await acquireCaptureStream(page.page, OPTIONS, { clock, deps: makeDeps(extension, server) });

    assert.deepEqual(timeline, [ "ready", "handler:on", "bringToFront", "down:Meta", "down:Shift", "press:KeyY", "up:Shift", "up:Meta", "tabs", "start" ],
      "readiness, then the handler, then the front, the grant keys in order, the query, and only then the start");

    const start = extension.calls.find((call) => call.kind === "start");

    assert.deepEqual(start?.arg, {

      audio: true,
      audioBitsPerSecond: 128000,
      frameSize: CAPTURE_FRAME_SIZE_MS,
      index: (start?.arg as ExtensionRecordingSettings).index,
      mimeType: "video/mp4;codecs=avc1,mp4a.40.2",
      tabId: 42,
      video: true,
      videoBitsPerSecond: 8000000,
      videoConstraints: OPTIONS.videoConstraints
    }, "the settings the extension receives are exactly the protocol's fields, with the caller's constraints carried through unchanged");
  });

  test("delivers the socket's messages in order, ignores another capture's socket, and ends on close", async () => {

    const timeline: string[] = [];
    const server = makeFakeServer(timeline);
    const extension = makeFakeExtension(timeline);
    const page = makeFakePage(timeline);
    const { clock } = makeFakeClock();
    const before = server.listeners();
    const stream = await acquireCaptureStream(page.page, OPTIONS, { clock, deps: makeDeps(extension, server) });
    const chunks: string[] = [];

    stream.on("data", (chunk: Buffer) => { chunks.push(chunk.toString()); });

    assert.equal(stream.readableHighWaterMark, CAPTURE_STREAM_HIGH_WATER_MARK, "the stream is sized to absorb Chrome's keyframe bursts");

    const index = (extension.calls.find((call) => call.kind === "start")?.arg as ExtensionRecordingSettings).index;

    // A socket announcing another capture's index belongs to another handler, so nothing of it reaches this stream.
    const stranger = makeFakeSocket(timeline);

    server.emitConnection(stranger, index + 1);
    stranger.emitMessage("not-ours");

    const socket = makeFakeSocket(timeline);

    server.emitConnection(socket, index);
    socket.emitMessage("first");
    socket.emitMessage("second");

    await new Promise((resolve) => setImmediate(resolve));

    assert.deepEqual(chunks, [ "first", "second" ], "only this capture's messages arrive, in order");

    // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
    const ended = Promise.withResolvers<void>();

    stream.once("end", () => { ended.resolve(); });

    socket.emitClose();

    await Promise.all([ ended.promise, stream.stopped ]);

    assert.equal(stream.readableEnded, true, "the socket's close ended the stream");
    assert.equal(server.listeners(), before, "the handler came off the server when the capture ended");
  });

  test("stop() asks the extension once across repeated calls and hands back the same promise every time", async () => {

    const timeline: string[] = [];
    const server = makeFakeServer(timeline);
    const extension = makeFakeExtension(timeline);
    const page = makeFakePage(timeline);
    const { clock } = makeFakeClock();
    const stream = await acquireCaptureStream(page.page, OPTIONS, { clock, deps: makeDeps(extension, server) });
    const first = stream.stop();
    const second = stream.stop();

    assert.equal(first, second, "every call resolves to the capture's own completion, not to the call's");

    await first;

    assert.equal(extension.calls.filter((call) => call.kind === "stop").length, 1, "the stop request goes out exactly once");
    assert.equal(server.listeners(), 0, "the handler came off the server");

    // A capture whose socket never connected has no close event coming, so its completion settles on the stop itself rather than waiting for one.
    await stream.stopped;
  });

  test("stop() on a disconnected browser resolves without asking the extension", async () => {

    // Negative test: the request has to reach a browser that is still listening. Evaluating against a dead one would reject inside a teardown that has nothing
    // useful to do with the rejection.
    const timeline: string[] = [];
    const server = makeFakeServer(timeline);
    const extension = makeFakeExtension(timeline);
    const page = makeFakePage(timeline);
    const { clock } = makeFakeClock();
    const stream = await acquireCaptureStream(page.page, OPTIONS, { clock, deps: makeDeps(extension, server) });

    extension.disconnect();

    await stream.stop();

    assert.equal(extension.calls.filter((call) => call.kind === "stop").length, 0, "no stop request is sent to a browser that is gone");
  });

  test("retries a source-unavailable refusal once, on a fresh index, and logs the window and tab state", async () => {

    /* The refusal arrives as a bare string, which is how puppeteer delivers a page-side rejection that is not an Error - classifying on .message would miss it
     * entirely. The retry has to be a wholly fresh attempt: the extension opens its socket before Chrome refuses, so reusing the index would leave the second
     * attempt sharing a socket with the first.
     */
    const timeline: string[] = [];
    const server = makeFakeServer(timeline);
    const before = server.listeners();
    const extension = makeFakeExtension(timeline, {

      onStart: async (_settings, attempt): Promise<void> => {

        if(attempt === 1) {

          // eslint-disable-next-line @typescript-eslint/only-throw-error -- Chrome's rejection arrives as a bare string, which is exactly what is under test.
          throw CAPTURE_SOURCE_UNAVAILABLE_MESSAGE;
        }
      }
    });
    const page = makeFakePage(timeline);
    const { clock } = makeFakeClock();

    let stream: Awaited<ReturnType<typeof acquireCaptureStream>> | null = null;

    const warnings = await captureWarnings(async () => {

      stream = await acquireCaptureStream(page.page, OPTIONS, { clock, deps: makeDeps(extension, server) });
    });

    const starts = extension.calls.filter((call) => call.kind === "start");

    assert.equal(starts.length, 2, "two starts: the refusal and the retry");
    assert.notEqual((starts[0]?.arg as ExtensionRecordingSettings).index, (starts[1]?.arg as ExtensionRecordingSettings).index,
      "the retry publishes under a fresh index");
    assert.ok(timeline.indexOf("handler:off") < timeline.lastIndexOf("handler:on"), "the failed attempt's handler came off before the retry registered its own");
    assert.equal(server.listeners(), before + 1, "exactly one handler survives - the successful attempt's");
    assert.equal(warnings.length, 1, "exactly one warning");
    assert.match(warnings[0]?.message ?? "", /could not start the tab capture on the first attempt/, "the warning names the refusal");
    assert.match(warnings[0]?.message ?? "", /attempt: 1/, "the warning carries which attempt refused");
    assert.match(warnings[0]?.message ?? "", /windowState: 'normal'/, "the warning carries the window state Chrome reported at the moment of the refusal");
    assert.match(warnings[0]?.message ?? "", /activeTab: 'https:\/\/example.test\/live'/, "and the tab the capture was aimed at");
    assert.ok(stream, "the acquisition returned the retry's stream");
  });

  test("a second source-unavailable refusal fails the acquisition and leaves nothing behind", async () => {

    const timeline: string[] = [];
    const server = makeFakeServer(timeline);
    const before = server.listeners();
    const extension = makeFakeExtension(timeline, {

      onStart: async (): Promise<void> => {

        // eslint-disable-next-line @typescript-eslint/only-throw-error -- Chrome's rejection arrives as a bare string.
        throw CAPTURE_SOURCE_UNAVAILABLE_MESSAGE;
      }
    });
    const page = makeFakePage(timeline);
    const { clock } = makeFakeClock();

    await assert.rejects(acquireCaptureStream(page.page, OPTIONS, { clock, deps: makeDeps(extension, server) }),
      (error: unknown) => (error instanceof Error) && error.message.includes(CAPTURE_SOURCE_UNAVAILABLE_MESSAGE),
      "the bare-string refusal reaches the caller as an Error carrying Chrome's text");

    assert.equal(extension.calls.filter((call) => call.kind === "start").length, CAPTURE_START_ATTEMPTS, "the acquisition stops at its attempt budget");
    assert.equal(server.listeners(), before, "every failed attempt's handler came off the server");
  });

  test("any other refusal fails after a single attempt, with no warning", async () => {

    // Negative test: the retry exists for one specific answer from Chrome. Retrying on everything would double the cost of every genuine failure and paper over
    // the collision the capture lock exists to prevent.
    const timeline: string[] = [];
    const server = makeFakeServer(timeline);
    const extension = makeFakeExtension(timeline, {

      onStart: async (): Promise<void> => {

        throw new Error("Cannot capture a tab with an active stream");
      }
    });
    const page = makeFakePage(timeline);
    const { clock } = makeFakeClock();

    let warnings: LogEntry[] = [];

    warnings = await captureWarnings(async () => {

      await assert.rejects(acquireCaptureStream(page.page, OPTIONS, { clock, deps: makeDeps(extension, server) }),
        (error: unknown) => (error instanceof Error) && (error.message === "Cannot capture a tab with an active stream"),
        "the rejection travels unchanged");
    });

    assert.equal(extension.calls.filter((call) => call.kind === "start").length, 1, "exactly one attempt");
    assert.equal(warnings.length, 0, "no retry warning for a failure that is not retried");
  });

  test("waits out a pending activeTab grant by re-attempting on the cadence, each on a fresh index", async () => {

    /* The keyboard command that grants activeTab is asynchronous, so Chrome can answer the first start with "has not been invoked for the current page". That
     * answer is a signal to ask again, and asking is strictly better than sleeping: a grant already in place costs one attempt and no wait at all.
     */
    const timeline: string[] = [];
    const server = makeFakeServer(timeline);
    const before = server.listeners();
    const extension = makeFakeExtension(timeline, {

      onStart: async (_settings, attempt): Promise<void> => {

        if(attempt < 3) {

          // eslint-disable-next-line @typescript-eslint/only-throw-error -- Chrome's rejection arrives as a bare string.
          throw "Extension has not been invoked for the current page (see activeTab permission)";
        }
      }
    });
    const page = makeFakePage(timeline);
    const { clock, sleeps } = makeFakeClock();

    await acquireCaptureStream(page.page, OPTIONS, { clock, deps: makeDeps(extension, server) });

    const starts = extension.calls.filter((call) => call.kind === "start");
    const indexes = starts.map((call) => (call.arg as ExtensionRecordingSettings).index);

    assert.equal(starts.length, 3, "the start is re-attempted until the grant lands");
    assert.equal(new Set(indexes).size, 3, "every attempt publishes under its own index");
    assert.deepEqual(sleeps, [ ACTIVE_TAB_GRANT_POLL_MS, ACTIVE_TAB_GRANT_POLL_MS ], "one cadence wait between each pair of attempts");
    assert.equal(server.listeners(), before + 1, "only the successful attempt's handler survives");
  });

  test("gives up on a grant that never lands and reports Chrome's own answer", async () => {

    const timeline: string[] = [];
    const server = makeFakeServer(timeline);
    const before = server.listeners();
    const extension = makeFakeExtension(timeline, {

      onStart: async (): Promise<void> => {

        // eslint-disable-next-line @typescript-eslint/only-throw-error -- Chrome's rejection arrives as a bare string.
        throw "Extension has not been invoked for the current page (see activeTab permission)";
      }
    });
    const page = makeFakePage(timeline);
    const { clock } = makeAdvancingClock();

    await assert.rejects(acquireCaptureStream(page.page, OPTIONS, { clock, deps: makeDeps(extension, server) }),
      (error: unknown) => (error instanceof Error) && error.message.includes("has not been invoked for the current page"),
      "the last grant-pending answer is what the caller sees");

    assert.equal(extension.calls.filter((call) => call.kind === "start").length, Math.floor(ACTIVE_TAB_GRANT_CEILING_MS / ACTIVE_TAB_GRANT_POLL_MS) + 1,
      "the ceiling affords one attempt plus one per cadence");
    assert.equal(server.listeners(), before, "no attempt left a handler behind");
  });

  test("destroying the stream stops the recording, and so does the page dying", async () => {

    // Both endings have to reach the extension: the owner's disposer destroys the stream, and a page that dies takes its capture with it. Neither can be left to
    // the socket handler, because a capture whose socket never connected would then never stop.
    const destroyTimeline: string[] = [];
    const destroyServer = makeFakeServer(destroyTimeline);
    const destroyExtension = makeFakeExtension(destroyTimeline);
    const destroyPage = makeFakePage(destroyTimeline);
    const destroyClock = makeFakeClock();
    const destroyed = await acquireCaptureStream(destroyPage.page, OPTIONS, { clock: destroyClock.clock, deps: makeDeps(destroyExtension, destroyServer) });

    destroyed.destroy();

    await destroyed.stopped;

    assert.equal(destroyExtension.calls.filter((call) => call.kind === "stop").length, 1, "destroying the stream stopped the recording");

    const closeTimeline: string[] = [];
    const closeServer = makeFakeServer(closeTimeline);
    const closeExtension = makeFakeExtension(closeTimeline);
    const closePage = makeFakePage(closeTimeline);
    const closeClock = makeFakeClock();
    const closed = await acquireCaptureStream(closePage.page, OPTIONS, { clock: closeClock.clock, deps: makeDeps(closeExtension, closeServer) });

    closePage.emitClose();

    await closed.stopped;

    assert.equal(closeExtension.calls.filter((call) => call.kind === "stop").length, 1, "the page's death stopped the recording");
  });

  test("rejects before preparing anything when the extension is not ready", async () => {

    // Negative test: the launch gate establishes readiness before a browser is published, so reaching this means the extension went away underneath one. Nothing
    // should be prepared or sent on that path.
    const timeline: string[] = [];
    const server = makeFakeServer(timeline);
    const extension = makeFakeExtension(timeline, { ready: false });
    const page = makeFakePage(timeline);
    const { clock } = makeFakeClock();

    await assert.rejects(acquireCaptureStream(page.page, OPTIONS, { clock, deps: makeDeps(extension, server) }),
      (error: unknown) => (error instanceof Error) && (error.message === EXTENSION_NOT_READY_MESSAGE));

    assert.deepEqual(timeline, ["ready"], "nothing was registered and no key was sent");
    assert.equal(server.listeners(), 0, "no handler was ever registered");
  });

  test("rejects when Chrome reports no capturable active tab, leaving no attempt behind", async () => {

    for(const tabs of [ [], [{ url: "https://example.test/live" }] ]) {

      const timeline: string[] = [];
      const server = makeFakeServer(timeline);
      const extension = makeFakeExtension(timeline, { tabs });
      const page = makeFakePage(timeline);
      const { clock } = makeFakeClock();

      // eslint-disable-next-line no-await-in-loop -- Each shape is a separate acquisition and has to settle before the next is set up.
      await assert.rejects(acquireCaptureStream(page.page, OPTIONS, { clock, deps: makeDeps(extension, server) }),
        (error: unknown) => (error instanceof Error) && (error.message === NO_ACTIVE_TAB_MESSAGE), "a tab without an id is no more capturable than no tab at all");

      assert.equal(extension.calls.filter((call) => call.kind === "start").length, 0, "no start was attempted");
      assert.equal(server.listeners(), 0, "the prepared attempt was discarded whole");
    }
  });

  test("never retries after the caller's signal has already aborted", async () => {

    /* The caller's deadline has fired by then, so a retry would run against a turn nobody is waiting on and could hand a live capture to a page that is already
     * closing. The refusal travels instead.
     */
    const timeline: string[] = [];
    const server = makeFakeServer(timeline);
    const controller = new AbortController();

    controller.abort();

    const extension = makeFakeExtension(timeline, {

      onStart: async (): Promise<void> => {

        // eslint-disable-next-line @typescript-eslint/only-throw-error -- Chrome's rejection arrives as a bare string.
        throw CAPTURE_SOURCE_UNAVAILABLE_MESSAGE;
      }
    });
    const page = makeFakePage(timeline);
    const { clock } = makeFakeClock();

    const warnings = await captureWarnings(async () => {

      await assert.rejects(acquireCaptureStream(page.page, OPTIONS, { clock, deps: makeDeps(extension, server), signal: controller.signal }),
        (error: unknown) => (error instanceof Error) && error.message.includes(CAPTURE_SOURCE_UNAVAILABLE_MESSAGE));
    });

    assert.equal(extension.calls.filter((call) => call.kind === "start").length, 1, "no retry runs once the caller has given up");
    assert.equal(warnings.length, 0, "and no retry warning is logged");
  });
});

/* The coupling pins. This module was written against a specific version of the library and its extension, and the protocol it speaks lives in files that a
 * dependency bump can change silently. These two rows read the INSTALLED files rather than a copy, so a bump fails here with instructions instead of failing a
 * capture in the field.
 */
describe("the pinned capture-extension protocol", () => {

  /**
   * Resolves a path inside the installed puppeteer-stream package.
   * @param relative - The path relative to the package root.
   * @returns The absolute path.
   */
  const libraryPath = (relative: string): string => path.join(path.dirname(fileURLToPath(import.meta.resolve("puppeteer-stream/package.json"))), relative);

  test("the library files this module speaks to are the ones it was written against", async () => {

    const fingerprints: Record<string, string> = {

      "dist/PuppeteerStream.js": "3dcc89ee5f3aa4ccc9180d670948b5eff49500e2b288889d81c19b8accbee2f7",
      "extension/manifest.json": "979bbad603000ff3dd5ec0159b198db6cab91f7dfa904e133e357c48010095b2",
      "extension/options.js": "243d332ecb333a9db13dc184cd97660b74f20186c172ce6b171c1db457f81ebf"
    };

    for(const [ relative, expected ] of Object.entries(fingerprints)) {

      // eslint-disable-next-line no-await-in-loop -- Three small reads; the sequence keeps the failure message pointing at one file.
      const actual = createHash("sha256").update(await readFile(libraryPath(relative))).digest("hex");

      assert.equal(actual, expected, "puppeteer-stream's " + relative + " has changed. Re-read extension/options.ts and dist/PuppeteerStream.js, re-verify " +
        "tabCapture.ts against them, and only then update this fingerprint.");
    }

    const manifest = JSON.parse(await readFile(libraryPath("package.json"), "utf8")) as { version: string };

    assert.equal(manifest.version, "3.0.23", "the pinned library version moved; re-verify the protocol before updating this pin");
  });

  test("the settings object carries exactly the fields the extension destructures", async () => {

    /* The extension's own source is the oracle. Listing the keys through a Record keyed on the interface makes the compiler insist every field is accounted for,
     * and comparing that list against the names parsed out of the extension's RecordingOptions catches a field added or removed upstream.
     */
    const settingKeys: Record<keyof ExtensionRecordingSettings, true> = {

      audio: true,
      audioBitsPerSecond: true,
      audioConstraints: true,
      bitsPerSecond: true,
      delay: true,
      frameSize: true,
      index: true,
      mimeType: true,
      tabId: true,
      video: true,
      videoBitsPerSecond: true,
      videoConstraints: true
    };

    const source = await readFile(libraryPath("extension/options.ts"), "utf8");
    const block = (/export type RecordingOptions = \{([\s\S]*?)\};/).exec(source);

    assert.ok(block?.[1], "the extension's RecordingOptions declaration could not be found; the protocol's shape cannot be verified");

    const declared = Array.from(block[1].matchAll(/^\s*(\w+)\??:/gm)).map((match) => match[1] ?? "").sort();

    assert.deepEqual(declared, Object.keys(settingKeys).sort(), "the extension's settings shape and this module's mirror of it have diverged");
  });
});
