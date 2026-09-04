/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * tokenExpiry.test.ts: Unit tests for the token-expiry strategy registry. Each strategy is exercised in isolation through parseTokenExpiry, the public dispatcher,
 * because the dispatcher's sole behavior is to fan out to TOKEN_EXPIRY_STRATEGIES in order. Tests are organized by strategy: one describe block per format with
 * happy-path, boundary, and ordering assertions; ordering is verified by constructing URLs that two strategies could in principle match and asserting that the
 * earlier-registered strategy wins.
 */
import { TOKEN_EXPIRY_STRATEGIES, parseTokenExpiry } from "./tokenExpiry.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

// The epoch-seconds instant every strategy row's token expires at. The URL fixtures keep their digits, because that is the text a real token carries; the
// assertions read the constant so the expected value and the fixture cannot drift apart.
const EXPIRY_SECONDS = 1730000000;

/* Encodes a JSON object as a base64url-encoded JWT segment so individual tests can synthesize a JWT payload without depending on a third-party library. The base64url
 * alphabet replaces "+" with "-", "/" with "_", and strips "=" padding; the encoder must produce that exact form because the JWT parser strategy decodes from it.
 */
function encodeJwtSegment(payload: object): string {

  return Buffer.from(JSON.stringify(payload), "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=/g, "");
}

/* Builds a complete three-segment JWT (header.payload.signature) with the given payload. The header and signature are placeholders since the strategies do not
 * verify the JWT - the test only needs the structural form so the parser can find and decode the payload.
 */
function buildJwt(payload: object): string {

  const header = encodeJwtSegment({ alg: "HS256", typ: "JWT" });
  const body = encodeJwtSegment(payload);

  return header + "." + body + ".synthetic-signature";
}

describe("parseTokenExpiry: plain ?exp= query parameter", () => {

  test("parses a 10-digit seconds-since-epoch value and normalizes to milliseconds", () => {

    // Happy path: the most common HLS CDN token shape. The strategy must recognize the parameter and multiply 10-digit values by 1000 since they are seconds.
    const url = "https://cdn.test/hls/master.m3u8?exp=1730000000";

    assert.equal(parseTokenExpiry(url), EXPIRY_SECONDS * 1000);
  });

  test("parses a 13-digit milliseconds-since-epoch value verbatim", () => {

    // Boundary: 13-digit values are already in milliseconds and must be returned without further multiplication. Locks the unit-detection branch.
    const url = "https://cdn.test/hls/master.m3u8?exp=1730000000123";

    assert.equal(parseTokenExpiry(url), 1730000000123);
  });

  test("parses ?exp= when it appears as a non-leading query parameter (&exp=)", () => {

    // Boundary: the regex anchor [?&] must accept both ?exp= (first parameter) and &exp= (subsequent parameter). Without the alternation the second case would
    // silently miss.
    const url = "https://cdn.test/master.m3u8?token=abc&exp=1730000000";

    assert.equal(parseTokenExpiry(url), EXPIRY_SECONDS * 1000);
  });

  test("returns null when the timestamp is fewer than 10 digits", () => {

    // Boundary: a 9-digit timestamp is below the regex's quantifier minimum and must not match. This protects against false positives where a non-timestamp
    // numeric parameter (e.g., a session counter) happens to have the name "exp".
    const url = "https://cdn.test/master.m3u8?exp=999999999";

    assert.equal(parseTokenExpiry(url), null);
  });

  test("returns null when the URL has no exp parameter and no other recognized format", () => {

    // Negative test: a URL that none of the strategies recognize must return null so the caller can decide not to schedule a refresh. Without this contract,
    // the caller would receive an arbitrary fallback and burn refresh cycles.
    assert.equal(parseTokenExpiry("https://cdn.test/master.m3u8"), null);
  });
});

describe("parseTokenExpiry: Akamai-style query token (~exp=N~)", () => {

  test("parses an Akamai hdnea-shaped token with the exp value bracketed by tildes", () => {

    // Happy path: the canonical Akamai hdnea encoding stuffs structured key=value pairs into a single query parameter, separated by tildes. The strategy must
    // extract the inner exp= value without being confused by the surrounding parameters.
    const url = "https://cdn.test/master.m3u8?hdnea=acl=/*~hmac=abc123~exp=1730000000~auth=xyz";

    assert.equal(parseTokenExpiry(url), EXPIRY_SECONDS * 1000);
  });
});

describe("parseTokenExpiry: Akamai-style path token (/exp=N~)", () => {

  test("parses a path-embedded token where the segments live in the URL path rather than a query parameter", () => {

    // Some Akamai deployments encode the token directly into the URL path. The strategy must recognize the leading slash to disambiguate from a plain query
    // parameter that happens to share the same numeric format.
    const url = "https://cdn.test/exp=1730000000~acl=/*/master.m3u8";

    assert.equal(parseTokenExpiry(url), EXPIRY_SECONDS * 1000);
  });
});

describe("parseTokenExpiry: URL-encoded hdnts (exp%3DN)", () => {

  test("parses an exp expressed with URL-encoded equals (%3D)", () => {

    // Some services pack a structured token into a single query value with internal "=" signs URL-encoded. The strategy must recognize "exp%3D" as the encoded
    // form and extract the timestamp.
    const url = "https://cdn.test/master.m3u8?hdnts=exp%3D1730000000~hmac%3Dabc";

    assert.equal(parseTokenExpiry(url), EXPIRY_SECONDS * 1000);
  });
});

describe("parseTokenExpiry: JWT in query parameter (Angelcam-style)", () => {

  test("extracts the exp claim from a JWT carried in a ?token= parameter", () => {

    // Happy path: the issue exemplar. Angelcam embeds a signed JWT in ?token=, and the strategy must split on dots, base64url-decode the payload, and read the
    // exp claim. The expiry comes back in seconds per RFC 7519, normalized to milliseconds by the dispatcher.
    const jwt = buildJwt({ did: "127256", exp: EXPIRY_SECONDS, iat: 1729999500 });
    const url = "https://cdn.test/path/playlist.m3u8?token=" + encodeURIComponent(jwt);

    assert.equal(parseTokenExpiry(url), EXPIRY_SECONDS * 1000);
  });

  test("works for parameter names other than 'token'", () => {

    // Boundary: services use varying parameter names for JWTs ("token", "auth", "jwt", "ticket", etc.). The strategy iterates every search-param value and should
    // not be hard-coded to a specific name. We use a synthetic name to verify the iteration covers all parameters.
    const jwt = buildJwt({ exp: EXPIRY_SECONDS });
    const url = "https://cdn.test/master.m3u8?credential=" + encodeURIComponent(jwt);

    assert.equal(parseTokenExpiry(url), EXPIRY_SECONDS * 1000);
  });

  test("ignores non-JWT query values and finds the JWT among other parameters", () => {

    // Boundary: a URL may carry multiple parameters where only one is a JWT. The strategy must skip non-JWT-shaped values without throwing and proceed to the
    // next parameter.
    const jwt = buildJwt({ exp: EXPIRY_SECONDS });
    const url = "https://cdn.test/master.m3u8?session=plain-string&counter=42&auth=" + encodeURIComponent(jwt);

    assert.equal(parseTokenExpiry(url), EXPIRY_SECONDS * 1000);
  });

  test("returns null when the JWT payload has no exp claim", () => {

    // Negative test: a JWT without an exp claim cannot inform a refresh schedule. The strategy must report null so the dispatcher continues to other strategies
    // (or returns null overall if nothing else matches).
    const jwt = buildJwt({ iat: 1729999500, sub: "user123" });
    const url = "https://cdn.test/master.m3u8?token=" + encodeURIComponent(jwt);

    assert.equal(parseTokenExpiry(url), null);
  });

  test("returns null for a malformed JWT (wrong segment count)", () => {

    // Negative test: a string with two dots that does not decode as a JWT (or has the wrong segment count) must not crash the parser. The strategy returns null
    // so the dispatcher continues.
    const url = "https://cdn.test/master.m3u8?token=" + encodeURIComponent("not.a.valid.jwt");

    assert.equal(parseTokenExpiry(url), null);
  });

  test("returns null for a JWT whose payload is not valid JSON", () => {

    // Negative test: a structurally-correct JWT (three segments, base64url-decodable) whose payload is not JSON must not throw. The JSON.parse failure is caught
    // and surfaces as null.
    const garbage = Buffer.from("this-is-not-json", "utf8")
      .toString("base64")
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=/g, "");
    const url = "https://cdn.test/master.m3u8?token=" + encodeURIComponent("eyJhbGciOiJIUzI1NiJ9." + garbage + ".sig");

    assert.equal(parseTokenExpiry(url), null);
  });

  test("returns null on a malformed URL that the URL constructor rejects", () => {

    // Negative test: the JWT strategy parses the URL through the WHATWG URL constructor; an input string that is not a valid URL must not crash. The strategy
    // returns null and the dispatcher reports null overall.
    assert.equal(parseTokenExpiry("not a url at all"), null);
  });

  test("parses the actual Angelcam URL shape from issue #34", () => {

    // Realistic fixture: the literal URL shape published in the open issue. The %2E sequences are URL-encoded dots that the URL constructor decodes for us
    // before the strategy splits the JWT segments. Locks the contract that this exact shape - the issue's exemplar - is recognized.
    const expSeconds = 1778289083;
    const payload = { did: "127256", exp: expSeconds, iat: 1778281883, nbf: 1778281763 };
    const headerSeg = encodeJwtSegment({ alg: "HS256", typ: "JWT" });
    const payloadSeg = encodeJwtSegment(payload);
    const sigSeg = "synthetic-signature";

    // Reproduce the URL-encoded-dot form Angelcam uses (%2E between segments). The URL constructor decodes %2E to "." when reading via searchParams, so the
    // JWT parser sees the canonical three-segment form.
    const tokenWithEncodedDots = headerSeg + "%2E" + payloadSeg + "%2E" + sigSeg;
    const url = "https://e1-na8.angelcam.com/cameras/127256/streams/hls/playlist.m3u8?token=" + tokenWithEncodedDots;

    assert.equal(parseTokenExpiry(url), expSeconds * 1000);
  });
});

describe("parseTokenExpiry: JWT payload structural guards", () => {

  test("returns null for a JWT whose payload decodes to valid JSON that is not an object (a bare number)", () => {

    // Negative test on the object guard in parseJwtExpClaim. The payload segment base64url-decodes to the string "42", which JSON.parse accepts as the number 42.
    // A scalar cannot carry an exp claim, so the "typeof claims !== object" branch must reject it and the strategy must report null rather than treating the bare
    // number as an expiry. We synthesize the numeric payload directly (encodeJwtSegment only encodes objects) using the same base64url transform the parser decodes.
    const numericPayload = Buffer.from("42", "utf8")
      .toString("base64")
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=/g, "");
    const header = encodeJwtSegment({ alg: "HS256", typ: "JWT" });
    const jwt = header + "." + numericPayload + ".synthetic-signature";
    const url = "https://cdn.test/master.m3u8?token=" + encodeURIComponent(jwt);

    assert.equal(parseTokenExpiry(url), null);
  });

  test("returns null for a JWT with an empty payload segment", () => {

    // Negative test on the empty-segment guard. A JWT with three segments but an empty middle one (header..signature) yields an empty payload string; the base64url
    // decoder short-circuits an empty segment to null, so the JSON parse is never attempted and parseJwtExpClaim returns null. This protects against a decode of the
    // empty string being misread as a valid (but exp-less) payload.
    const header = encodeJwtSegment({ alg: "HS256", typ: "JWT" });
    const jwt = header + "..synthetic-signature";
    const url = "https://cdn.test/master.m3u8?token=" + encodeURIComponent(jwt);

    assert.equal(parseTokenExpiry(url), null);
  });
});

describe("parseTokenExpiry: strategy ordering", () => {

  test("plain ?exp= takes precedence over a JWT with a different exp value when both are present", () => {

    // Boundary on the strategy registry: the dispatcher iterates TOKEN_EXPIRY_STRATEGIES in order and returns the first non-null result. We construct a URL that
    // both the plain regex strategy and the JWT strategy could match (a ?exp= query param plus a separate ?token= JWT), and assert that the earlier-registered
    // strategy wins. A reordering that broke this would be detectable as a value mismatch.
    const jwt = buildJwt({ exp: 9999999999 });
    const url = "https://cdn.test/master.m3u8?exp=1730000000&token=" + encodeURIComponent(jwt);

    assert.equal(parseTokenExpiry(url), EXPIRY_SECONDS * 1000, "plain ?exp= wins because it is registered first");
  });
});

describe("TOKEN_EXPIRY_STRATEGIES registry", () => {

  test("declares each strategy with a unique stable name", () => {

    // Locks the registry against silent dupes. The names appear in debug logs; a duplicate would make filtering by strategy name unreliable.
    const seen = new Set<string>();

    for(const strategy of TOKEN_EXPIRY_STRATEGIES) {

      assert.ok(strategy.name, "strategy declares a non-empty name");
      assert.ok(!seen.has(strategy.name), "strategy name '" + strategy.name + "' is unique");
      seen.add(strategy.name);
    }
  });

  test("every strategy returns null for a URL that none of them should recognize", () => {

    // Boundary: a generic URL with no recognizable token format must produce null from every individual strategy. Without this check, a strategy that
    // accidentally matches everything would silently shadow the others on every URL.
    const innocuousUrl = "https://cdn.test/path/manifest.m3u8?session=xyz&format=hls";

    for(const strategy of TOKEN_EXPIRY_STRATEGIES) {

      assert.equal(strategy.parse(innocuousUrl), null, "strategy '" + strategy.name + "' does not match the innocuous URL");
    }
  });
});
