/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * chromeFetch.test.ts: Unit tests for the Chrome User-Agent fetch wrapper in chromeFetch.ts. The module holds a single piece of mutable state (currentUserAgent)
 * mutated through the setter; tests reset it between cases. The fetch boundary is replaced via mock.method on globalThis so we can inspect the URL/headers
 * passed to fetch without making real network calls.
 */
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { chromeFetch, getChromeUserAgent, setChromeUserAgent } from "./chromeFetch.ts";
import assert from "node:assert/strict";

describe("setChromeUserAgent and getChromeUserAgent", () => {

  afterEach(() => {

    // Always clear the global UA after each test so cross-test leakage cannot occur.
    setChromeUserAgent(null);
  });

  test("getChromeUserAgent returns null before any setter call (initial state)", () => {

    setChromeUserAgent(null);

    assert.equal(getChromeUserAgent(), null);
  });

  test("setChromeUserAgent stores the provided UA and getChromeUserAgent reads it back", () => {

    setChromeUserAgent("Mozilla/5.0 ChromeTest/123");

    assert.equal(getChromeUserAgent(), "Mozilla/5.0 ChromeTest/123");
  });

  test("setChromeUserAgent(null) clears a previously-set UA", () => {

    setChromeUserAgent("Mozilla/5.0 ChromeTest/123");
    setChromeUserAgent(null);

    assert.equal(getChromeUserAgent(), null, "null clears state");
  });

  test("the latest set call wins when called multiple times", () => {

    setChromeUserAgent("first");
    setChromeUserAgent("second");
    setChromeUserAgent("third");

    assert.equal(getChromeUserAgent(), "third");
  });
});

describe("chromeFetch", () => {

  // Track the last call to globalThis.fetch so the tests can assert on the URL/init that was passed.
  let lastUrl: string | URL = "";
  let lastInit: RequestInit | undefined;

  beforeEach(() => {

    lastUrl = "";
    lastInit = undefined;

    // Replace global fetch with a stub that records and returns a synthetic Response. mock.method reverts on mock.reset so we don't have to manage the original.
    mock.method(globalThis, "fetch", async (url: string | URL, init?: RequestInit): Promise<Response> => {

      lastUrl = url;
      lastInit = init;

      return new Response("ok", { status: 200 });
    });
  });

  afterEach(() => {

    setChromeUserAgent(null);
    mock.reset();
  });

  test("falls through to plain fetch when no UA is configured (no header injection)", async () => {

    setChromeUserAgent(null);

    await chromeFetch("https://example.test/path");

    assert.equal(lastUrl, "https://example.test/path", "URL passes through verbatim");

    // The plain-fetch branch passes init unchanged, including undefined.
    assert.equal(lastInit, undefined, "init is undefined when caller passes nothing");
  });

  test("preserves caller-supplied init when no UA is configured", async () => {

    setChromeUserAgent(null);
    const callerInit: RequestInit = { body: "payload", method: "POST" };

    await chromeFetch("https://example.test/path", callerInit);

    assert.equal(lastInit, callerInit, "init reference is forwarded directly when there's no UA injection");
  });

  test("injects the User-Agent header when a UA is configured", async () => {

    setChromeUserAgent("Mozilla/5.0 ChromeTest/123");

    await chromeFetch("https://example.test/path");

    const headers = lastInit?.headers as Headers;

    assert.ok(headers instanceof Headers, "headers were normalized to a Headers instance");
    assert.equal(headers.get("User-Agent"), "Mozilla/5.0 ChromeTest/123", "Chrome UA injected when caller provided no UA");
  });

  test("does NOT overwrite a caller-supplied User-Agent header", async () => {

    // Negative test: caller-provided UA wins. The function must not clobber explicit caller intent.
    setChromeUserAgent("ChromeUA");

    await chromeFetch("https://example.test/path", { headers: { "User-Agent": "CallerUA" } });

    const headers = lastInit?.headers as Headers;

    assert.equal(headers.get("User-Agent"), "CallerUA", "caller's UA preserved over the configured Chrome UA");
  });

  test("respects a caller-supplied lowercase 'user-agent' header (case-insensitive lookup)", async () => {

    // Boundary: HTTP header names are case-insensitive per RFC 7230, and the Headers constructor normalizes them. The has("User-Agent") check must therefore
    // match a caller-supplied "user-agent" key. A regression that compared via plain object key lookup would inject ChromeUA over the caller's lowercase UA -
    // this test catches that.
    setChromeUserAgent("ChromeUA");

    await chromeFetch("https://example.test/path", { headers: { "user-agent": "LowercaseCallerUA" } });

    const headers = lastInit?.headers as Headers;

    assert.equal(headers.get("User-Agent"), "LowercaseCallerUA", "lowercase caller UA preserved (Headers.has is case-insensitive)");
  });

  test("respects a caller-supplied mixed-case 'USER-AGENT' header", async () => {

    // Symmetric: an all-caps or other unusual casing must also be recognized as a user-supplied UA and not be overwritten. The Headers constructor is the
    // single normalization point.
    setChromeUserAgent("ChromeUA");

    await chromeFetch("https://example.test/path", { headers: { "USER-AGENT": "AllCapsCallerUA" } });

    const headers = lastInit?.headers as Headers;

    assert.equal(headers.get("User-Agent"), "AllCapsCallerUA", "mixed-case caller UA preserved");
  });

  test("merges caller-supplied non-UA headers with the injected UA", async () => {

    setChromeUserAgent("ChromeUA");

    await chromeFetch("https://example.test/path", {

      headers: {

        "Accept": "application/json",
        "X-Custom": "value"
      }
    });

    const headers = lastInit?.headers as Headers;

    assert.equal(headers.get("User-Agent"), "ChromeUA", "UA injected");
    assert.equal(headers.get("Accept"), "application/json", "caller's Accept preserved");
    assert.equal(headers.get("X-Custom"), "value", "caller's custom header preserved");
  });

  test("normalizes Headers-instance input on the caller-provided headers", async () => {

    // Boundary: caller can pass a Headers instance directly. The Headers constructor accepts another Headers instance and copies it.
    setChromeUserAgent("ChromeUA");

    const callerHeaders = new Headers();

    callerHeaders.set("X-Custom", "custom-value");

    await chromeFetch("https://example.test/path", { headers: callerHeaders });

    const headers = lastInit?.headers as Headers;

    assert.equal(headers.get("X-Custom"), "custom-value");
    assert.equal(headers.get("User-Agent"), "ChromeUA");
  });

  test("normalizes array-of-pairs headers input", async () => {

    setChromeUserAgent("ChromeUA");

    await chromeFetch("https://example.test/path", { headers: [[ "X-Custom", "v" ]] });

    const headers = lastInit?.headers as Headers;

    assert.equal(headers.get("X-Custom"), "v");
    assert.equal(headers.get("User-Agent"), "ChromeUA");
  });

  test("preserves init fields other than headers (method, body, signal)", async () => {

    setChromeUserAgent("ChromeUA");

    const controller = new AbortController();

    await chromeFetch("https://example.test/path", {

      body: "request-body",
      method: "PUT",
      signal: controller.signal
    });

    const init = lastInit!;

    assert.equal(init.method, "PUT", "method preserved");
    assert.equal(init.body, "request-body", "body preserved");
    assert.equal(init.signal, controller.signal, "signal preserved by reference");
  });

  test("accepts a URL instance as the first argument", async () => {

    setChromeUserAgent("ChromeUA");

    const url = new URL("https://example.test/foo");

    await chromeFetch(url);

    assert.equal(lastUrl, url, "URL instance forwarded by reference");
  });

  test("returns the Response from the underlying fetch", async () => {

    setChromeUserAgent("ChromeUA");

    const response = await chromeFetch("https://example.test/path");

    assert.equal(response.status, 200);
    assert.equal(await response.text(), "ok");
  });
});
