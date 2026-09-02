/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tabCapture.ts: PrismCast's own conversation with the capture extension.
 *
 * Starting a tab capture is four steps against the browser: bring the page to the front, send the keyboard command that grants the extension its activeTab
 * permission for that page, ask which tab is active, and hand the extension a settings object it turns into a MediaRecorder feeding a WebSocket. Every piece
 * those steps need - the extension's options page, the WebSocket server, the browser launch - is exported by puppeteer-stream, so PrismCast speaks the protocol
 * itself rather than through a convenience wrapper, and three things follow from that.
 *
 * There is one capture lock in this process, and streaming/setup.ts owns it. A wrapper that carries a lock of its own would serialize the same work twice, and a
 * rejection escaping such a lock's unguarded section leaves it held for the life of the process - a condition no Chrome restart can clear, because the state is
 * in Node rather than in the browser. Owning the start means the only serialization is the one whose deadline, abort signal, and wedge policy the caller already
 * controls.
 *
 * A rejected start is a retry rather than an outage. Chrome answers a start it cannot serve with "Could not start video source", and that answer is worth one
 * more attempt against a fresh socket index; a second refusal is a real failure carrying the window and active-tab state that explains it. The activeTab grant
 * gets the same treatment: rather than sleeping a fixed interval after the keyboard command and hoping the grant landed, the start is attempted and Chrome's own
 * "has not been invoked for the current page" answer drives a short poll, so a grant that lands immediately costs nothing.
 *
 * The coupling this module takes on is the extension's protocol, and it is pinned rather than trusted: tabCapture.test.ts fingerprints the three library files
 * this module was written against and derives the settings-object shape from the extension's own source, so a dependency bump that changes either fails the
 * suite with instructions rather than failing a capture in the field.
 */
import { LOG, formatError, pollUntil, realClock, startTimer } from "../utils/index.ts";
import type { RawData, WebSocket, WebSocketServer } from "ws";
import { getExtensionPage, wss } from "puppeteer-stream";
import type { Clock } from "../utils/index.ts";
import type { IncomingMessage } from "node:http";
import type { Nullable } from "../types/index.ts";
import type { Page } from "puppeteer-core";
import { PassThrough } from "node:stream";
import type { Readable } from "node:stream";
import { readWindowState } from "./cdp.ts";

/* The extension's options page publishes these three names on its global scope, and they exist only there. Declaring them at module scope rather than in the
 * project's global declarations is deliberate: a global START_RECORDING would typecheck anywhere in the Node program and fail only when it ran, while these
 * declarations keep the names spellable in exactly the one file whose evaluate callbacks execute inside that page.
 */
declare function START_RECORDING(settings: ExtensionRecordingSettings): Promise<void>;
declare function STOP_RECORDING(index: number): Promise<void>;
declare const chrome: { tabs: { query(query: { active: boolean }): Promise<{ id?: number; url?: string }[]> } };

// Constants.

// The extension's readiness predicate, stated once. The launch gate waits on it before publishing a browser, and the acquisition asserts it before preparing an
// attempt, so both ask the same question of the same page.
export const EXTENSION_READY_EXPRESSION = "typeof START_RECORDING === 'function'";

// Chrome's rejection text when it cannot open a capture source for the tab. This is the failure the retry exists for.
export const CAPTURE_SOURCE_UNAVAILABLE_MESSAGE = "Could not start video source";

// Chrome's rejection text when the extension's activeTab grant has not landed for the page yet. The keyboard command that grants it is asynchronous, so this
// answer means "ask again shortly" rather than "this cannot work".
export const ACTIVE_TAB_GRANT_PENDING_MESSAGE = "has not been invoked for the current page";

// The failure when a browser is serving without a loaded capture extension. The launch gate establishes readiness before a browser is published, so reaching
// this means the extension went away underneath a published browser.
export const EXTENSION_NOT_READY_MESSAGE = "The capture extension is not ready on this browser.";

// The failure when Chrome reports no active tab to capture. tabCapture always targets the active tab, so there is nothing to point the recorder at.
export const NO_ACTIVE_TAB_MESSAGE = "No active tab was found for capture.";

// How many times a start is attempted before the acquisition fails. Two: the first attempt, and one retry for the source-unavailable answer that a fresh attempt
// against a fresh socket index has been observed to clear.
export const CAPTURE_START_ATTEMPTS = 2;

// The cadence between start attempts while Chrome is still reporting the activeTab grant pending, and the ceiling on that wait. A grant that lands with the
// keyboard command costs one attempt and no wait at all; the ceiling bounds the case where it never lands, so the caller sees Chrome's own message rather than
// a hang. One attempt is a handful of CDP round trips - the front, the five key events, the tab query, the start - so the ceiling is sized to afford several of
// them rather than to a multiple of the cadence alone.
export const ACTIVE_TAB_GRANT_POLL_MS = 50;
export const ACTIVE_TAB_GRANT_CEILING_MS = 1000;

// The recorder's timeslice in milliseconds - how often the extension's MediaRecorder hands a chunk to its socket. A browser-side recorder setting rather than a
// wait of ours, at the value the capture pipeline was tuned against.
export const CAPTURE_FRAME_SIZE_MS = 20;

// The capture stream's buffer ceiling. Chrome delivers bursty multi-megabyte chunks on keyframes, so the buffer is sized to absorb a burst without the socket's
// writes backing up into the recorder.
export const CAPTURE_STREAM_HIGH_WATER_MARK = 8 * 1024 * 1024;

// Types.

/**
 * The dimension and frame-rate bounds a capture track is held to. Chrome reads these as tabCapture's mandatory media constraints, which is what pins the encoder
 * to the surface the page was emulated at rather than to whatever the window happens to be showing.
 */
export interface CaptureVideoConstraints {

  readonly mandatory: {

    readonly maxFrameRate: number;
    readonly maxHeight: number;
    readonly maxWidth: number;
    readonly minFrameRate: number;
    readonly minHeight: number;
    readonly minWidth: number;
  };
}

/**
 * What a caller asks a capture for. videoConstraints is required rather than optional because every caller supplies one and it is the only thing tying the
 * encoder to the emulated surface; a capture acquired without it would silently compose whatever the window was presenting.
 */
export interface CaptureStreamOptions {

  readonly audio: boolean;
  readonly audioBitsPerSecond?: number;
  readonly mimeType: string;
  readonly video: boolean;
  readonly videoBitsPerSecond?: number;
  readonly videoConstraints: CaptureVideoConstraints;
}

/**
 * The settings object the extension's START_RECORDING receives, mirroring the RecordingOptions type in the extension's own source field for field.
 *
 * One divergence is deliberate. The extension annotates audioBitsPerSecond, videoBitsPerSecond, and bitsPerSecond as required numbers, but it hands all three
 * straight to MediaRecorder, where an absent value selects the recorder's own default - and the readiness probe passes none of them and captures perfectly well.
 * So this type declares what the protocol tolerates rather than what the extension's annotation over-claims, and the settings-shape test pins the field NAMES
 * against the extension's source so a genuine protocol change still fails loudly.
 */
export interface ExtensionRecordingSettings {

  audio: boolean;
  audioBitsPerSecond?: number;
  audioConstraints?: CaptureVideoConstraints;
  bitsPerSecond?: number;
  delay?: number;
  frameSize: number;
  index: number;
  mimeType: string;
  tabId: number;
  video: boolean;
  videoBitsPerSecond?: number;
  videoConstraints?: CaptureVideoConstraints;
}

/**
 * A live capture: the readable stream of recorder chunks, plus the two members that let an owner end it and know when the ending is complete.
 */
export interface CaptureStream extends Readable {

  // Asks the extension to stop the recording. Safe to call more than once - the request goes out once and every call resolves to the same stopped promise below.
  stop(): Promise<void>;

  // Resolves once this capture has genuinely finished: the extension closed its socket after the recorder stopped, its tracks stopped, and its pending sends
  // drained; or, for a capture whose socket never connected, once stop() has run. It never rejects, so a caller bounds it rather than catching it.
  readonly stopped: Promise<void>;
}

/**
 * The two library values this module talks to, injected so a test can drive the whole acquisition without a browser or a listening socket.
 */
export interface TabCaptureDeps {

  readonly getExtensionPage: typeof getExtensionPage;
  readonly wss: typeof wss;
}

/**
 * Per-call context for an acquisition: the clock its waits run on, the collaborators it talks through, and the caller's abort signal.
 */
export interface AcquireCaptureStreamContext {

  // The time port driving the grant poll and the elapsed measurements. Defaults to realClock; tests inject a fake.
  readonly clock?: Clock;

  // The library collaborators. Defaults to the real ones.
  readonly deps?: TabCaptureDeps;

  // The caller's own deadline signal. A retry never starts after it has fired, because by then the caller has already given up on this acquisition.
  readonly signal?: AbortSignal;
}

/* One start attempt's resources: the socket index it published under, the server listener it registered, the socket that connected for it, the signal for its
 * completion, and the stream its chunks land in. Every attempt gets its own, and an attempt that did not start is discarded whole.
 */
interface CaptureAttempt {

  readonly index: number;
  readonly onConnection: (socket: WebSocket, request: IncomingMessage) => void;
  socket: Nullable<WebSocket>;
  readonly stopped: PromiseWithResolvers<void>;
  readonly stream: PassThrough;
}

// The active tab a start was aimed at, reduced to the two fields anything here reads.
interface ActiveTab {

  readonly id: number;
  readonly url: Nullable<string>;
}

/* What one attempt produced. A started attempt carries its resources forward; a grant-pending attempt carries only the rejection that said so, because its
 * resources were discarded before this value was returned. Every other rejection throws rather than becoming an outcome.
 */
type AttemptOutcome = { attempt: CaptureAttempt; kind: "started"; tab: ActiveTab } | { error: unknown; kind: "grant-pending" };

// The production collaborators.
export const defaultTabCaptureDeps: TabCaptureDeps = { getExtensionPage, wss };

// The socket index each capture publishes under. It only ever moves forward, so no two captures in this process - including two attempts of the same
// acquisition - can be confused for one another on the shared server.
let nextCaptureIndex = 0;

// Acquisition.

/**
 * Normalizes anything thrown into an Error. Puppeteer delivers a page-side exception that is not an Error object as the primitive itself, and the extension
 * rejects its capture-start with a bare string, so a rethrow that assumed an Error would strip the only diagnostic there was.
 * @param error - The thrown value.
 * @returns The value itself when it is already an Error, otherwise an Error carrying its formatted text.
 */
function toError(error: unknown): Error {

  return (error instanceof Error) ? error : new Error(formatError(error));
}

/**
 * Prepares one start attempt: a fresh socket index, a fresh stream, and a connection handler registered on the server BEFORE the extension is invoked. The
 * ordering is required, not tidy - START_RECORDING opens its socket before it asks Chrome for the capture, so a handler registered afterwards would miss the
 * connection, and a start that goes on to fail still connects a socket for its index.
 * @param server - The WebSocket server the extension connects back to.
 * @returns The prepared attempt.
 */
function prepareAttempt(server: WebSocketServer): CaptureAttempt {

  const index = nextCaptureIndex++;
  const stream = new PassThrough({ highWaterMark: CAPTURE_STREAM_HIGH_WATER_MARK });
  // eslint-disable-next-line @typescript-eslint/no-invalid-void-type -- Standard pattern for signal promises.
  const stopped = Promise.withResolvers<void>();

  const attempt: CaptureAttempt = {

    index,
    onConnection: (socket: WebSocket, request: IncomingMessage): void => {

      // The extension names the capture it is connecting for in the query string. Any other index belongs to another capture's handler.
      if(new URL(request.url ?? "/", "http://localhost").searchParams.get("index") !== String(index)) {

        return;
      }

      attempt.socket = socket;

      socket.on("message", (data: RawData) => {

        if(!stream.destroyed && !stream.writableEnded) {

          stream.write(data);
        }
      });

      socket.on("close", () => {

        if(!stream.destroyed && !stream.writableEnded) {

          stream.end();
        }

        // The extension closes its socket only after the recorder stopped and its pending sends drained, so this close is the completion signal an owner waits
        // on before closing the page out from under it.
        stopped.resolve();
        server.off("connection", attempt.onConnection);
      });
    },
    socket: null,
    stopped,
    stream
  };

  server.on("connection", attempt.onConnection);

  return attempt;
}

/**
 * Discards an attempt that did not start: its listener comes off the server, its socket is closed if the extension managed to open one, its stream is destroyed,
 * and its completion signal settles. Nothing of a failed attempt outlives this call, which is what lets the next attempt begin from a clean server.
 * @param server - The WebSocket server the attempt registered on.
 * @param attempt - The attempt to discard.
 */
function discardAttempt(server: WebSocketServer, attempt: CaptureAttempt): void {

  server.off("connection", attempt.onConnection);
  attempt.socket?.close();
  attempt.stream.destroy();
  attempt.stopped.resolve();
}

/**
 * Sends the keyboard command the extension's manifest binds, which is what grants it activeTab permission for the page in front.
 * @param page - The page the capture is being acquired for.
 */
async function grantActiveTab(page: Page): Promise<void> {

  const modifier = (process.platform === "darwin") ? "Meta" : "Control";

  await page.keyboard.down(modifier);
  await page.keyboard.down("Shift");
  await page.keyboard.press("KeyY");
  await page.keyboard.up("Shift");
  await page.keyboard.up(modifier);
}

/**
 * Asks the extension which tab is active. tabCapture always targets the active tab, so this is the tab the recorder will be pointed at.
 * @param extension - The extension's options page.
 * @returns The active tab.
 * @throws When Chrome reports no active tab, or one without an id to capture.
 */
async function queryActiveTab(extension: Page): Promise<ActiveTab> {

  const [tab] = await extension.evaluate(() => chrome.tabs.query({ active: true }));

  if(tab?.id === undefined) {

    throw new Error(NO_ACTIVE_TAB_MESSAGE);
  }

  return { id: tab.id, url: tab.url ?? null };
}

/**
 * Runs one START_RECORDING evaluate against the extension for a prepared attempt.
 * @param extension - The extension's options page.
 * @param attempt - The prepared attempt whose index and stream the recording feeds.
 * @param options - What the caller asked the capture for.
 * @param tabId - The tab Chrome will capture.
 */
async function startRecording(extension: Page, attempt: CaptureAttempt, options: CaptureStreamOptions, tabId: number): Promise<void> {

  const settings: ExtensionRecordingSettings = {

    audio: options.audio,
    audioBitsPerSecond: options.audioBitsPerSecond,
    frameSize: CAPTURE_FRAME_SIZE_MS,
    index: attempt.index,
    mimeType: options.mimeType,
    tabId,
    video: options.video,
    videoBitsPerSecond: options.videoBitsPerSecond,
    videoConstraints: options.videoConstraints
  };

  await extension.evaluate((recording: ExtensionRecordingSettings) => START_RECORDING(recording), settings);
}

/**
 * Reads the two pieces of state that explain a capture-start refusal. Both are best-effort: a diagnostic that threw would replace the failure it was meant to
 * describe.
 * @param page - The page the capture was being acquired for.
 * @param extension - The extension's options page.
 * @returns The active tab's URL and the window's reported state, either of which may be null when it could not be read.
 */
async function readAcquisitionDiagnostics(page: Page, extension: Page): Promise<{ activeTab: Nullable<string>; windowState: Nullable<string> }> {

  const windowState = await readWindowState(page);

  let activeTab: Nullable<string> = null;

  try {

    const [tab] = await extension.evaluate(() => chrome.tabs.query({ active: true }));

    activeTab = tab?.url ?? null;
  } catch(error) {

    LOG.debug("browser:lifecycle", "The active tab could not be read for the capture-start diagnostic: %s.", formatError(error));
  }

  return { activeTab, windowState };
}

/**
 * Attempts one start, waiting out an activeTab grant that has not landed yet. The keyboard command that grants the permission is asynchronous, so rather than
 * sleeping a fixed interval and hoping, this attempts the start and reads Chrome's own answer: "has not been invoked for the current page" means ask again on
 * the cadence, anything else is this attempt's verdict. A grant already in place costs exactly one attempt and no wait.
 *
 * Each poll iteration prepares a wholly fresh attempt and discards it before the next, so no listener, socket, or index is ever reused across a refusal.
 * @param page - The page to capture.
 * @param options - What the caller asked the capture for.
 * @param collaborators - The extension page, the socket server, and the clock the cadence runs on.
 * @param collaborators.clock - The clock driving the grant cadence.
 * @param collaborators.extension - The extension's options page.
 * @param collaborators.server - The WebSocket server the extension connects back to.
 * @returns The attempt that started, with the tab it was aimed at.
 * @throws The last grant-pending rejection when the ceiling lapses, or the attempt's own rejection for any other failure.
 */
async function acquireOnce(page: Page, options: CaptureStreamOptions,
  collaborators: { clock: Clock; extension: Page; server: WebSocketServer }): Promise<{ attempt: CaptureAttempt; tab: ActiveTab }> {

  const { clock, extension, server } = collaborators;

  const runAttempt = async (): Promise<AttemptOutcome> => {

    const attempt = prepareAttempt(server);

    try {

      await page.bringToFront();
      await grantActiveTab(page);

      const tab = await queryActiveTab(extension);

      await startRecording(extension, attempt, options, tab.id);

      return { attempt, kind: "started", tab };
    } catch(error) {

      discardAttempt(server, attempt);

      if(formatError(error).includes(ACTIVE_TAB_GRANT_PENDING_MESSAGE)) {

        return { error, kind: "grant-pending" };
      }

      throw toError(error);
    }
  };

  const outcome = await pollUntil({ cadenceMs: ACTIVE_TAB_GRANT_POLL_MS, ceilingMs: ACTIVE_TAB_GRANT_CEILING_MS, clock, read: runAttempt,
    until: (result: AttemptOutcome): boolean => result.kind === "started" });

  if(outcome.value.kind === "grant-pending") {

    throw toError(outcome.value.error);
  }

  return { attempt: outcome.value.attempt, tab: outcome.value.tab };
}

/**
 * Attaches the two capture-control members to a started attempt's stream, producing the CaptureStream its owner holds.
 * @param attempt - The attempt that started.
 * @param extension - The extension's options page, which STOP_RECORDING is evaluated against.
 * @param server - The WebSocket server the attempt registered on.
 * @returns The owner-facing capture stream.
 */
function attachCaptureControls(attempt: CaptureAttempt, extension: Page, server: WebSocketServer): CaptureStream {

  let stopping = false;

  const requestStop = async (): Promise<void> => {

    // The stop request has to reach a browser that is still listening. When it is already gone the recording is gone with it, so this is the one path where
    // asking is pointless rather than merely best-effort.
    if(!extension.isClosed() && extension.browser().connected) {

      try {

        await extension.evaluate((index: number) => STOP_RECORDING(index), attempt.index);
      } catch(error) {

        LOG.debug("browser:lifecycle", "The capture extension did not accept the stop request: %s.", formatError(error));
      }
    }

    server.off("connection", attempt.onConnection);

    // A capture whose socket never connected has no close event coming, so its completion signal settles here rather than waiting for one that cannot arrive.
    if(!attempt.socket) {

      attempt.stopped.resolve();
    }
  };

  /* stop() is deliberately not async: every call has to hand back the SAME promise object, because the completion an owner waits on is the capture's, not this
   * call's. The request itself goes out once, guarded by the flag, and its own failures are swallowed inside requestStop so this promise never rejects.
   */
  const stop = (): Promise<void> => {

    if(!stopping) {

      stopping = true;

      void requestStop();
    }

    return attempt.stopped.promise;
  };

  return Object.assign(attempt.stream, { stop, stopped: attempt.stopped.promise });
}

/**
 * Acquires a tab capture for a page: one started recording, its chunks arriving as a readable stream, and the two controls that end it.
 *
 * A start Chrome refuses with "Could not start video source" is retried once against a fresh attempt, with the window's reported state and the active tab logged
 * so the refusal explains itself. A second refusal, any other rejection, and a refusal arriving after the caller's signal has aborted all fail the acquisition
 * unchanged - there is nothing left to retry into.
 * @param page - The page to capture.
 * @param options - What the capture is asked for.
 * @param context - The clock, collaborators, and caller abort signal. Defaults to the production collaborators on the real clock, with no signal.
 * @returns The started capture.
 * @throws When the extension is not ready, no tab is active, or the start fails on every attempt.
 */
export async function acquireCaptureStream(page: Page, options: CaptureStreamOptions,
  context: AcquireCaptureStreamContext = {}): Promise<CaptureStream> {

  const { clock = realClock, deps = defaultTabCaptureDeps, signal } = context;
  const acquisitionElapsed = startTimer(clock);
  const extension = await deps.getExtensionPage(page.browser());
  const server = await deps.wss;

  /* The launch gate already established the extension's readiness before this browser was published, so this asks once rather than retrying: reaching it means
   * the extension went away underneath a published browser, which is a fault to report rather than a condition to wait out. It reads the same statement of the
   * predicate the gate waits on.
   */
  const ready = await extension.evaluate(EXTENSION_READY_EXPRESSION);

  if(ready !== true) {

    throw new Error(EXTENSION_NOT_READY_MESSAGE);
  }

  let failure: unknown;

  for(let attempt = 1; attempt <= CAPTURE_START_ATTEMPTS; attempt++) {

    try {

      // eslint-disable-next-line no-await-in-loop -- The attempts are a retry sequence: each has to fail before the next is worth making.
      const started = await acquireOnce(page, options, { clock, extension, server });
      const stream = attachCaptureControls(started.attempt, extension, server);

      /* Both close paths end the recording. The owner's disposer destroys the stream, which emits close; a page that dies takes its capture with it and fires
       * the page's own close. Registering here rather than inside the socket handler means a capture whose socket never connected still stops cleanly.
       */
      page.once("close", () => { void stream.stop(); });
      stream.once("close", () => { void stream.stop(); });

      LOG.debug("timing:startup", "Capture acquired in %dms (attempt %d of %d).", acquisitionElapsed(), attempt, CAPTURE_START_ATTEMPTS);

      return stream;
    } catch(error) {

      failure = error;

      // A refusal is worth another attempt only while there is one left to make and the caller is still waiting for it. Everything else is this acquisition's
      // verdict, and it travels unchanged.
      if(!formatError(error).includes(CAPTURE_SOURCE_UNAVAILABLE_MESSAGE) || (attempt >= CAPTURE_START_ATTEMPTS) || signal?.aborted) {

        break;
      }

      // eslint-disable-next-line no-await-in-loop -- The diagnostics describe the refusal that just happened, so they are read before the retry, not alongside it.
      const diagnostics = await readAcquisitionDiagnostics(page, extension);

      LOG.warn("Chrome could not start the tab capture on the first attempt; retrying once.",
        { activeTab: diagnostics.activeTab, attempt, elapsedMs: acquisitionElapsed(), windowState: diagnostics.windowState });
    }
  }

  // The failure is already an Error: acquireOnce normalizes every rejection at the one boundary where a page-side value enters, so it travels unchanged.
  throw failure;
}
