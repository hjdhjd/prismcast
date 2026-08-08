/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * setup.test.ts: Unit tests for the synchronous, testable surface of the stream setup module - StreamSetupError, generateStreamId, shouldReverifyCapture,
 * validateStreamUrl, and withSignInGuidance - all of which earn full coverage here, plus verifyManifestSelection, which is async only because its one await is a
 * caller-supplied promise: it touches no browser, so it is covered here rather than deferred. The Chrome-entangled async exports (createPageWithCapture,
 * reestablishChannelManifest, setupStream, verifyCaptureSystem) drive a real Chrome browser via Puppeteer and FFmpeg subprocess; their happy paths require
 * integration fixtures and are deferred to e2e - for the re-establishment that means its step bounds, its conditional log-context wrap, and its settlement-
 * attached re-mute are diff-read facts, with the consequences a caller can observe pinned in native/index.refresh.test.ts. We cover every throw reachable from
 * the synchronous surface (StreamSetupError construction and validateStreamUrl rejections).
 */
import { StreamSetupError, generateStreamId, shouldReverifyCapture, validateStreamUrl, verifyManifestSelection, withSignInGuidance } from "./setup.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { loadHealthState, markDomainAuth, markDomainAuthRequired } from "../config/health.ts";
import { mkdtemp, rm } from "node:fs/promises";
import type { ManifestInterceptionResult } from "../browser/manifestInterceptor.ts";
import type { Nullable } from "../types/index.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../testing.helpers.ts";
import { initializeDataDir } from "../config/paths.ts";
import { initializeUserChannels } from "../config/userChannels.ts";
import { makeProfile } from "../config/profiles.helpers.ts";
import os from "node:os";
import path from "node:path";

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

    // UrlValidationResult carries reason as an optional string field, so it is directly readable on the failure result; there is no discriminated-union narrowing here.
    const reason = (result as { reason?: string }).reason;

    assert.equal(reason, "URL is required.");
  });

  test("rejects empty string input with 'URL is required.'", () => {

    // Boundary: empty string is also caught by the leading falsy check.
    const result = validateStreamUrl("");

    assert.equal(result.valid, false);

    // UrlValidationResult carries reason as an optional string field, so it is directly readable on the failure result; there is no discriminated-union narrowing here.
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

describe("withSignInGuidance", () => {

  let dir: string;

  beforeEach(async () => {

    dir = await mkdtemp(path.join(os.tmpdir(), "prismcast-guidance-test-"));
    initializeDataDir(dir);
    await initializeUserChannels();

    // Reload health state from the fresh (empty) data dir so domain auth residue from other test files cannot leak into the guidance decision.
    await loadHealthState();

    // We mock Date for deterministic timestamps and setTimeout to suppress the 2-second debounced health flush timer the mark calls below schedule.
    mock.timers.enable({ apis: [ "Date", "setTimeout" ], now: 1_700_000_000_000 });
  });

  afterEach(async () => {

    mock.timers.reset();
    await rm(dir, { force: true, recursive: true });
  });

  test("leads the message with sign-in guidance naming the service when the channel's domain is marked needs-sign-in", () => {

    /* Traced path: the channel key resolves through resolveServiceKey to the predefined "abc" channel, whose URL extracts to the abc.com auth domain; the flagged
     * entry satisfies the needsLogin status check, so the guidance sentence is prepended. Dropping the prepend (or the state read) would return the message
     * unchanged and the leading-guidance assertion below would fail. The underlying error must remain identifiable at the end of the message.
     */
    markDomainAuthRequired("abc.com");

    const message = withSignInGuidance("Failed to start stream.", "abc", "ABC");

    assert.equal(message, "ABC needs sign-in. Open PrismCast's channel table and click this channel's login icon to sign in. Failed to start stream.");
  });

  test("returns the message unchanged when the domain is verified", () => {

    // Traced path: the needsLogin status comparison - a verified entry must not trigger guidance.
    markDomainAuth("abc.com");

    assert.equal(withSignInGuidance("Failed to start stream.", "abc", "ABC"), "Failed to start stream.");
  });

  test("returns the message unchanged when the domain has no auth state", () => {

    assert.equal(withSignInGuidance("Failed to start stream.", "abc", "ABC"), "Failed to start stream.");
  });

  test("returns the message unchanged for ad-hoc URL streams with no channel identity", () => {

    // Traced path: the falsy channelKey early return - ad-hoc /play streams pass undefined and must never consult domain auth.
    assert.equal(withSignInGuidance("Failed to start stream.", undefined, "Example"), "Failed to start stream.");
    assert.equal(withSignInGuidance("Failed to start stream.", null, "Example"), "Failed to start stream.");
  });

  test("returns the message unchanged when the channel key cannot resolve to a domain (empty-domain guard)", () => {

    /* Traced path: getAuthDomainForChannel returns an empty string for an unknown key, and the empty-domain guard must short-circuit before the state read - an
     * unguarded lookup against "" could match a malformed entry and mislabel an unrelated failure.
     */
    assert.equal(withSignInGuidance("Failed to start stream.", "definitely-not-a-channel-key", "Example"), "Failed to start stream.");
  });
});

describe("shouldReverifyCapture", () => {

  test("returns true when nothing is in flight, a browser is available, and no other stream is active (empty registry)", () => {

    // Traced path: the common real-world shape - setup fails before the stream is even registered, so activeStreamIds is empty. [].every(...) is vacuously
    // true, and a regression that swapped every for some (or checked length !== 0) would flip this to false.
    assert.equal(shouldReverifyCapture({ activeStreamIds: [], failingStreamId: 7, hasBrowser: true, reverificationInProgress: false }), true);
  });

  test("returns false when a re-verification is already in progress, even with every other input passing", () => {

    // Traced path: the single-flight guard must win regardless of the isolation and browser checks. A regression that dropped this term (or reordered the
    // short-circuit incorrectly) would return true here.
    assert.equal(shouldReverifyCapture({ activeStreamIds: [], failingStreamId: 7, hasBrowser: true, reverificationInProgress: true }), false);
  });

  test("returns false when an active stream id other than the failing stream is present", () => {

    // Traced path: the isolation check must block re-verification whenever any other stream is active, since that stream is either demonstrably capturing or
    // will drain on its own. A regression that used some(...) instead of every(...), or dropped the exclusion entirely, would return true here.
    assert.equal(shouldReverifyCapture({ activeStreamIds: [ 7, 12 ], failingStreamId: 7, hasBrowser: true, reverificationInProgress: false }), false);
  });

  test("returns false when no browser is available to probe", () => {

    // Traced path: a readiness probe needs a connected browser to exercise. A regression that dropped this term would return true here.
    assert.equal(shouldReverifyCapture({ activeStreamIds: [], failingStreamId: 7, hasBrowser: false, reverificationInProgress: false }), false);
  });

  test("returns true when the only active stream id is the failing stream's own entry", () => {

    // Traced path: the failing stream's own registry entry must not count as "another stream" - the exclusion (id === failingStreamId) is what makes this
    // the only-active-stream case, not an empty list. A regression that dropped the exclusion (e.g. checking activeStreamIds.length === 0) would return false.
    assert.equal(shouldReverifyCapture({ activeStreamIds: [7], failingStreamId: 7, hasBrowser: true, reverificationInProgress: false }), true);
  });

  test("returns true for an empty registry, the common real case where setup fails before the stream is even registered", () => {

    // Traced path: this is the PARITY-critical assertion pinned separately from the general happy-path test above - an empty registry must not regress to
    // false, since this is the ordinary shape of a capture-infrastructure failure (it fails during setup, before getNextStreamId's entry is registered).
    assert.equal(shouldReverifyCapture({ activeStreamIds: [], failingStreamId: 42, hasBrowser: true, reverificationInProgress: false }), true);
  });
});

/* The Fox CDN master URL shape the live verifier decodes: the second-to-last path segment carries the call sign and region, so this URL reads as "fnc". Every
 * row below that reaches the verifier is checked against these two fixtures rather than a hand-written expectation, so the rows travel with the extractor.
 */
const FOX_MASTER_URL = "https://cdn.example.test/abc123/prod/fnc-ue2/index.m3u8";

/* A chunklist URL for a different call sign, "fs1". The kind gate is what keeps this away from the verifier, and a URL the verifier WOULD reject is what makes
 * the media-kind row prove the gate: a row carrying a URL the verifier accepts anyway would pass whether or not the gate exists.
 */
const FOX_MEDIA_URL = "https://cdn.example.test/abc123/prod/fs1-ue2/chunklist.m3u8";

/* A thenable that records whether anything consumed it. The pre-await gating rows assert that a profile with nothing to verify returns without touching the
 * interception at all, which is the latency half of the contract - a helper that awaited first and gated second would return the same value while making every
 * unverifiable stream wait out the interceptor's settle. Awaiting invokes then(), so the flag is the observation.
 */
function makeWatchedInterception(value: Nullable<ManifestInterceptionResult>): { promise: Promise<Nullable<ManifestInterceptionResult>>; wasConsumed: () => boolean } {

  const settled = Promise.resolve(value);
  let consumed = false;

  const promise = {

    then: (onFulfilled?: ((v: Nullable<ManifestInterceptionResult>) => unknown) | null,
      onRejected?: ((reason: unknown) => unknown) | null): Promise<unknown> => {

      consumed = true;

      return settled.then(onFulfilled, onRejected);
    }
  } as Promise<Nullable<ManifestInterceptionResult>>;

  return { promise, wasConsumed: (): boolean => consumed };
}

describe("verifyManifestSelection", () => {

  test("resolves null when the interception is null, without reading a kind off nothing", async () => {

    // Traced path: a verifier-bearing profile with a selector passes the pre-await gate, so the null interception is what the kind gate has to survive. A
    // regression that dropped the null-safe access would throw a TypeError here rather than resolving.
    const result = await verifyManifestSelection(Promise.resolve(null), makeProfile({ channelSelection: { strategy: "foxGrid" }, channelSelector: "FNC" }));

    assert.equal(result, null);
  });

  test("resolves null for a media-kind selection, so a chunklist URL never reaches a master-calibrated verifier", async () => {

    /* Traced path: the fixture URL decodes to a different call sign than the selector, so the live Fox verifier would return a mismatch reason if the kind gate
     * let it through. A regression that inverted the kind comparison therefore fails this row with a reason instead of null.
     */
    const interception: ManifestInterceptionResult = { manifestUrl: FOX_MEDIA_URL, selectedKind: "media" };
    const result = await verifyManifestSelection(Promise.resolve(interception),
      makeProfile({ channelSelection: { strategy: "foxGrid" }, channelSelector: "FNC" }));

    assert.equal(result, null);
  });

  test("resolves null without consulting the interception when the provider has no verifier", async () => {

    // Traced path: guideGrid is a registered provider that implements no verifier, so the gate is the missing hook rather than a missing provider.
    const watched = makeWatchedInterception({ manifestUrl: FOX_MASTER_URL, selectedKind: "master" });
    const result = await verifyManifestSelection(watched.promise, makeProfile({ channelSelection: { strategy: "guideGrid" }, channelSelector: "FNC" }));

    assert.equal(result, null);
    assert.equal(watched.wasConsumed(), false, "a stream with no verifier must not wait on the interception");
  });

  test("resolves null without consulting the interception when the profile carries no channel selector", async () => {

    // Traced path: the verifier exists but has no expected channel to compare against, so there is nothing to verify and nothing to wait for.
    const watched = makeWatchedInterception({ manifestUrl: FOX_MASTER_URL, selectedKind: "master" });
    const result = await verifyManifestSelection(watched.promise, makeProfile({ channelSelection: { strategy: "foxGrid" }, channelSelector: null }));

    assert.equal(result, null);
    assert.equal(watched.wasConsumed(), false, "a profile with no selector must not wait on the interception");
  });

  test("resolves null when the live Fox verifier accepts a master URL for the requested call sign", async () => {

    // Traced path: the full pass-through - gates admit, kind admits, and the provider's own verifier decodes "fnc" from the URL and matches the selector.
    const interception: ManifestInterceptionResult = { manifestUrl: FOX_MASTER_URL, selectedKind: "master" };
    const result = await verifyManifestSelection(Promise.resolve(interception),
      makeProfile({ channelSelection: { strategy: "foxGrid" }, channelSelector: "FNC" }));

    assert.equal(result, null);
  });

  test("returns the live Fox verifier's reason when the master URL belongs to a different call sign", async () => {

    /* Traced path: the same URL against a different selector. The reason string is the provider's, returned through the helper unchanged, which is what makes
     * this the pass-through half of the pair rather than a second acceptance row.
     */
    const interception: ManifestInterceptionResult = { manifestUrl: FOX_MASTER_URL, selectedKind: "master" };
    const result = await verifyManifestSelection(Promise.resolve(interception),
      makeProfile({ channelSelection: { strategy: "foxGrid" }, channelSelector: "FS1" }));

    assert.equal(result, "Manifest URL is for channel \"fnc\", but \"FS1\" was requested.");
  });
});
