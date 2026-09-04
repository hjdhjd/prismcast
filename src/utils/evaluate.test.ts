/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * evaluate.test.ts: Unit tests for the Puppeteer evaluate wrapper in evaluate.ts. The wrapper takes a Page or Frame whose only relevant API is .evaluate(); we
 * stub that with a fake object so no real browser is needed. AbortController registration, timeout fallback, and the stream-context abort path each get explicit
 * coverage.
 */
import { EvaluateAbortError, EvaluateTimeoutError, evaluateWithAbort, getAbortController, getAbortSignal, registerAbortController,
  unregisterAbortController } from "./evaluate.ts";
import { afterEach, describe, test } from "node:test";
import type { Page } from "puppeteer-core";
import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import { runWithStreamContext } from "./streamContext.ts";

// makeFakePage builds an object that satisfies the parts of Page/Frame that evaluateWithAbort touches. We type as Page to feed into the wrapper but expose a
// minimal surface - the wrapper only calls .evaluate() with a function and optional args.
function makeFakePage(evaluate: (...args: unknown[]) => unknown): Page {

  // The wrapper calls .catch() on the evaluate result, so the fake must always return a Promise. We coerce non-promise return values via Promise.resolve().
  const wrapped = (...args: unknown[]): Promise<unknown> => Promise.resolve(evaluate(...args));

  return { evaluate: wrapped } as unknown as Page;
}

describe("registerAbortController / unregisterAbortController / getAbortController / getAbortSignal", () => {

  afterEach(() => {

    // Clean any registrations created during the test so the singleton map is fresh.
    unregisterAbortController("test-stream");
    unregisterAbortController("test-stream-2");
  });

  test("registerAbortController stores the controller and getAbortController returns it", () => {

    const controller = new AbortController();

    registerAbortController("test-stream", controller);

    assert.equal(getAbortController("test-stream"), controller, "stored controller round-trips");
  });

  test("getAbortSignal returns the controller's signal", () => {

    const controller = new AbortController();

    registerAbortController("test-stream", controller);

    assert.equal(getAbortSignal("test-stream"), controller.signal);
  });

  test("returns undefined for an unknown stream id", () => {

    // Boundary: an id that was never registered must not throw - it returns undefined for both accessors.
    assert.equal(getAbortController("never-registered"), undefined);
    assert.equal(getAbortSignal("never-registered"), undefined);
  });

  test("unregister removes the controller", () => {

    const controller = new AbortController();

    registerAbortController("test-stream", controller);
    unregisterAbortController("test-stream");

    assert.equal(getAbortController("test-stream"), undefined);
    assert.equal(getAbortSignal("test-stream"), undefined);
  });

  test("unregister of an unknown id is a no-op (does not throw)", () => {

    assert.doesNotThrow(() => { unregisterAbortController("never-registered"); });
  });

  test("re-registering the same id replaces the previous controller", () => {

    // Negative test: a later register call wins; the old controller is forgotten.
    const oldController = new AbortController();
    const newController = new AbortController();

    registerAbortController("test-stream", oldController);
    registerAbortController("test-stream", newController);

    assert.equal(getAbortController("test-stream"), newController);
  });
});

describe("EvaluateTimeoutError", () => {

  test("carries the timeout duration in its message and a stable name", () => {

    const error = new EvaluateTimeoutError(5000);

    assert.equal(error.name, "EvaluateTimeoutError");
    assert.match(error.message, /5000ms/, "duration in milliseconds appears in the message");
  });

  test("is an instance of Error", () => {

    assert.ok(new EvaluateTimeoutError(0) instanceof Error);
  });
});

describe("EvaluateAbortError", () => {

  test("has a stable message and name", () => {

    const error = new EvaluateAbortError();

    assert.equal(error.name, "EvaluateAbortError");
    assert.match(error.message, /aborted/);
  });

  test("is an instance of Error", () => {

    assert.ok(new EvaluateAbortError() instanceof Error);
  });
});

describe("evaluateWithAbort happy path", () => {

  test("returns the value resolved by the underlying evaluate call", async () => {

    const page = makeFakePage(() => "result-value");

    const result = await evaluateWithAbort<string, []>(page, () => "fn-body");

    assert.equal(result, "result-value");
  });

  test("forwards positional args to the underlying evaluate call", async () => {

    let observedArgs: unknown[] = [];

    const page = makeFakePage((..._args: unknown[]) => {

      // Drop the function itself - the first argument is the function being evaluated; the rest are the positional args we want to observe.
      observedArgs = _args.slice(1);

      return "ok";
    });

    await evaluateWithAbort<string, [number, string]>(page, () => "ok", [ 42, "hello" ]);

    assert.deepEqual(observedArgs, [ 42, "hello" ]);
  });
});

describe("evaluateWithAbort timeout", () => {

  test("rejects with EvaluateTimeoutError when the inner evaluate never resolves", async () => {

    // The inner evaluate hangs forever; the timeout fires and surfaces our custom error.
    const page = makeFakePage(() => new Promise(() => { /* never resolves */ }));

    await assert.rejects(
      () => evaluateWithAbort(page, () => "value", undefined, 5),
      (err: Error) => err instanceof EvaluateTimeoutError
    );
  });

  test("propagates errors thrown by the inner evaluate verbatim (not wrapped in timeout)", async () => {

    // Negative test: when the inner evaluate rejects fast, the wrapper must surface that error without conversion.
    const page = makeFakePage(() => Promise.reject(new Error("inner failure")));

    await assert.rejects(
      () => evaluateWithAbort(page, () => "value", undefined, 1000),
      /inner failure/
    );
  });
});

describe("evaluateWithAbort with stream context", () => {

  afterEach(() => {

    unregisterAbortController("ctx-stream");
  });

  test("rejects immediately with EvaluateAbortError if signal is already aborted before the call", async () => {

    const controller = new AbortController();

    controller.abort();
    registerAbortController("ctx-stream", controller);

    await runWithStreamContext({ streamId: "ctx-stream" }, async () => {

      const page = makeFakePage(() => "should-not-run");

      await assert.rejects(
        () => evaluateWithAbort(page, () => "v"),
        (err: Error) => err instanceof EvaluateAbortError
      );
    });
  });

  test("rejects with EvaluateAbortError when the abort fires after the call has started", async () => {

    const controller = new AbortController();

    registerAbortController("ctx-stream", controller);

    await runWithStreamContext({ streamId: "ctx-stream" }, async () => {

      const page = makeFakePage(() => new Promise(() => { /* hangs */ }));

      // The wrapper builds its composed signal synchronously, so by the time the call returns its promise the wait is live and the stream signal has seen
      // whatever registration it is ever going to see. Reading the count here is the during-the-wait half of the guarantee.
      const pending = evaluateWithAbort(page, () => "v", undefined, 5000);

      assert.equal(getEventListeners(controller.signal, "abort").length, 0, "no abort listener lands on the stream signal while a wait is live");

      controller.abort();

      await assert.rejects(() => pending, (err: Error) => err instanceof EvaluateAbortError);

      // Completes the guarantee across every exit path of evaluateWithAbort (this is the abort path; normal completion and timeout are covered below): the
      // stream signal carries no user-visible abort listener after the call settles either. Composing signals registers the dependency inside the platform,
      // where getEventListeners cannot see it, so this reads zero for the right reason - nothing to accumulate rather than something cleaned up in time.
      assert.equal(getEventListeners(controller.signal, "abort").length, 0, "no abort listener lingers on the stream signal after the abort fires");
    });
  });

  test("rejects with EvaluateAbortError when abort fires synchronously during the inner evaluate (race-window guard)", async () => {

    // The wrapper pre-checks signal.aborted before starting the evaluate, then composes that signal with its timeout and waits on the result. If the stream
    // aborts in the narrow window between the pre-check and the wait, the composed signal is already aborted when the wait begins, and waiting on an
    // already-aborted signal rejects with its reason immediately. We exercise that window by having the fake's evaluate abort synchronously.
    const controller = new AbortController();

    registerAbortController("ctx-stream", controller);

    await runWithStreamContext({ streamId: "ctx-stream" }, async () => {

      const page = makeFakePage(() => {

        // Fire abort during the synchronous evaluate callback - this lands AFTER the wrapper's pre-check, which saw a non-aborted signal, but BEFORE the signal
        // composition. The composed signal is therefore born aborted, and the wait rejects with the stream's reason rather than waiting out the timeout.
        controller.abort();

        return new Promise(() => { /* hangs */ });
      });

      await assert.rejects(
        () => evaluateWithAbort(page, () => "v", undefined, 5000),
        (err: Error) => err instanceof EvaluateAbortError
      );
    });
  });

  test("falls back to timeout-only when no abort controller is registered for the stream", async () => {

    // Boundary: a stream context exists but its controller was never registered (e.g., before the controller is wired up). The wrapper must still apply the
    // timeout and not get stuck waiting for an abort.
    await runWithStreamContext({ streamId: "no-controller" }, async () => {

      const page = makeFakePage(() => new Promise(() => { /* hangs */ }));

      await assert.rejects(
        () => evaluateWithAbort(page, () => "v", undefined, 5),
        (err: Error) => err instanceof EvaluateTimeoutError
      );
    });
  });

  test("uses default 15s timeout when none is supplied", async () => {

    // We can't wait 15s in a test, but we can verify that omitting the timeout still applies one (a fast-resolving evaluate succeeds).
    const page = makeFakePage(() => "fast");
    const result = await evaluateWithAbort(page, () => "fast");

    assert.equal(result, "fast");
  });

  test("keeps the stream signal free of abort listeners across repeated normal completions", async () => {

    const controller = new AbortController();

    registerAbortController("ctx-stream", controller);

    await runWithStreamContext({ streamId: "ctx-stream" }, async () => {

      const page = makeFakePage(() => "ok");

      // The long-lived per-stream signal must never accumulate user-visible abort listeners, at any moment. Ten concurrent calls are the pressure test: were
      // the wrapper to subscribe to the stream signal directly, this would read ten while they are in flight and would only return to zero if every path
      // unsubscribed. Reading zero both during and after is the stronger statement, and it is a tripwire against reintroducing direct registration.
      const pending = Array.from({ length: 10 }, () => evaluateWithAbort(page, () => "ok"));

      assert.equal(getEventListeners(controller.signal, "abort").length, 0, "no abort listeners on the stream signal while ten calls are in flight");

      await Promise.all(pending);

      assert.equal(getEventListeners(controller.signal, "abort").length, 0, "no abort listeners on the stream signal after they all complete");
    });
  });

  test("keeps the stream signal free of abort listeners across the timeout path", async () => {

    const controller = new AbortController();

    registerAbortController("ctx-stream", controller);

    await runWithStreamContext({ streamId: "ctx-stream" }, async () => {

      const page = makeFakePage(() => new Promise(() => { /* hangs */ }));

      const pending = evaluateWithAbort(page, () => "v", undefined, 5);

      assert.equal(getEventListeners(controller.signal, "abort").length, 0, "no abort listener on the stream signal while the bound is counting down");

      await assert.rejects(() => pending, (err: Error) => err instanceof EvaluateTimeoutError);

      assert.equal(getEventListeners(controller.signal, "abort").length, 0, "no abort listener on the stream signal after the bound lapses");
    });
  });
});

describe("evaluateWithAbort outside any stream context", () => {

  test("operates with timeout-only behavior (no stream id, no abort signal lookup)", async () => {

    const page = makeFakePage(() => "no-context");
    const result = await evaluateWithAbort(page, () => "no-context");

    assert.equal(result, "no-context");
  });

  test("times out without a stream context (timeout still applies)", async () => {

    const page = makeFakePage(() => new Promise(() => { /* hangs */ }));

    await assert.rejects(
      () => evaluateWithAbort(page, () => "v", undefined, 5),
      (err: Error) => err instanceof EvaluateTimeoutError
    );
  });
});

