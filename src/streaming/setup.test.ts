/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.test.ts: Unit tests for the synchronous, testable surface of the stream setup module. setup.ts has three synchronously testable pure exports -
 * StreamSetupError, generateStreamId, and validateStreamUrl - all of which earn full coverage here. The async exports (createPageWithCapture, setupStream,
 * verifyCaptureSystem) drive a real Chrome browser via Puppeteer and FFmpeg subprocess; their happy paths require integration fixtures and are deferred to e2e.
 * We cover every throw reachable from the synchronous surface (StreamSetupError construction and validateStreamUrl rejections).
 */
import { StreamSetupError, generateStreamId, validateStreamUrl } from "./setup.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("StreamSetupError", () => {

  test("constructs with message, statusCode, and userMessage exposed as readonly fields", () => {

    const err = new StreamSetupError("internal log message", 503, "user-facing message");

    assert.equal(err.message, "internal log message");
    assert.equal(err.statusCode, 503);
    assert.equal(err.userMessage, "user-facing message");
    assert.equal(err.name, "StreamSetupError", "name set so error logs identify the class");
  });

  test("is an instance of Error (so try/catch with instanceof works as expected)", () => {

    const err = new StreamSetupError("x", 500, "y");

    assert.ok(err instanceof Error);
    assert.ok(err instanceof StreamSetupError);
  });

  test("preserves a 400-class statusCode for client errors", () => {

    const err = new StreamSetupError("bad URL", 400, "Invalid URL.");

    assert.equal(err.statusCode, 400);
  });

  test("preserves a 503-class statusCode for capacity errors", () => {

    // 503 is the documented status for capacity-reached and capture-infrastructure errors. Locks the contract that callers can branch on err.statusCode.
    const err = new StreamSetupError("capacity reached", 503, "Maximum streams reached.");

    assert.equal(err.statusCode, 503);
  });
});

describe("generateStreamId", () => {

  test("returns a channel-prefixed ID when channelName is provided", () => {

    // The implementation forms "<channel>-<6charRequest>". We assert on the prefix and that the suffix is exactly 6 alphanumeric characters.
    const id = generateStreamId("nbc", "https://nbc.com/live");

    assert.match(id, /^nbc-[a-z0-9]{6}$/);
  });

  test("returns a domain-prefixed ID when only URL is provided", () => {

    // Falls back to extractDomain(url) when channelName is undefined. Locks the domain prefix path used for ad-hoc streams.
    const id = generateStreamId(undefined, "https://watch.foo.example/live");

    assert.match(id, /^foo\.example-[a-z0-9]{6}$/);
  });

  test("returns 'unknown-' prefix when neither channel nor URL is provided", () => {

    // Boundary: both inputs are undefined. The implementation falls through to the literal "unknown-" prefix.
    const id = generateStreamId(undefined, undefined);

    assert.match(id, /^unknown-[a-z0-9]{6}$/);
  });

  test("produces distinct IDs across sequential calls (random suffix)", () => {

    // The 6-character random suffix gives 36^6 = 2.1B combinations. Two calls must produce distinct IDs to give logs traceability.
    const a = generateStreamId("nbc", "https://nbc.com");
    const b = generateStreamId("nbc", "https://nbc.com");

    assert.notEqual(a, b, "two sequential calls produce distinct request IDs");
  });

  test("preserves channel name even when URL is empty", () => {

    // Boundary: channelName takes priority over URL. Empty URL should not affect the channel-prefixed path.
    const id = generateStreamId("espn", "");

    assert.match(id, /^espn-[a-z0-9]{6}$/);
  });

  test("uses the URL fallback when channel name is the empty string (falsy)", () => {

    // Boundary: an empty-string channel name is falsy, so the function falls through to the URL branch. Locks the falsy-vs-undefined consistency.
    const id = generateStreamId("", "https://hulu.com");

    assert.match(id, /^hulu\.com-[a-z0-9]{6}$/);
  });
});

describe("validateStreamUrl", () => {

  test("accepts an https URL", () => {

    const result = validateStreamUrl("https://example.test/live");

    assert.equal(result.valid, true);
  });

  test("accepts an http URL", () => {

    const result = validateStreamUrl("http://example.test/live");

    assert.equal(result.valid, true);
  });

  test("accepts a chrome: URL (used for internal pages like chrome://gpu)", () => {

    // Chrome internal URLs are explicitly listed in allowedProtocols. Locks the diagnostic-page support contract.
    const result = validateStreamUrl("chrome://gpu");

    assert.equal(result.valid, true);
  });

  test("rejects undefined input with 'URL is required.'", () => {

    // Negative test: undefined falls into the early-return branch.
    const result = validateStreamUrl(undefined);

    assert.equal(result.valid, false);

    // The discriminant is now narrowed to false at the type level - dereferencing result.reason is type-safe without a guard.
    const reason = (result as { reason?: string }).reason;

    assert.equal(reason, "URL is required.");
  });

  test("rejects empty string input with 'URL is required.'", () => {

    // Boundary: empty string is also caught by the leading falsy check.
    const result = validateStreamUrl("");

    assert.equal(result.valid, false);

    // The discriminant is now narrowed to false at the type level - dereferencing result.reason is type-safe without a guard.
    const reason = (result as { reason?: string }).reason;

    assert.equal(reason, "URL is required.");
  });

  test("rejects javascript: URLs as unsupported protocols", () => {

    // Negative test: protocol-block list. javascript: URLs would let attackers exfiltrate cookies via XSS-style navigation; rejection is non-negotiable.
    const result = validateStreamUrl("javascript:alert('x')");

    assert.equal(result.valid, false);
    assert.match((result as { reason?: string }).reason ?? "", /Unsupported protocol/);
  });

  test("rejects data: URLs as unsupported protocols", () => {

    const result = validateStreamUrl("data:text/html,<script>alert(1)</script>");

    assert.equal(result.valid, false);
  });

  test("rejects file: URLs as unsupported protocols", () => {

    // Negative test: file:// URLs would expose the server's filesystem. Must be rejected.
    const result = validateStreamUrl("file:///etc/passwd");

    assert.equal(result.valid, false);
    assert.match((result as { reason?: string }).reason ?? "", /Unsupported protocol/);
  });

  test("rejects malformed URL strings with 'Invalid URL format.'", () => {

    // Negative test: the URL constructor throws for unparseable input. The catch branch surfaces the documented message.
    const result = validateStreamUrl("not a url at all");

    assert.equal(result.valid, false);
    assert.equal((result as { reason?: string }).reason, "Invalid URL format.");
  });

  test("includes the protocol in the rejection reason for diagnostics", () => {

    const result = validateStreamUrl("ftp://example.test/file");

    assert.equal(result.valid, false);
    assert.match((result as { reason?: string }).reason ?? "", /ftp:/, "rejected protocol surfaced in the reason");
  });
});
