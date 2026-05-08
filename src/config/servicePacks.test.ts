/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * servicePacks.test.ts: Unit tests for the service-pack import/export module. The pure parseServicePack pipeline does heavy validation and sanitization;
 * tests cover the version gate, missing-field detection, channel sub-validation, and the legacy flag normalization that runs before profile validation.
 * exportServicePack is also exercised against the loaded user-profile state - which in unit tests is empty - so the test focuses on the null-return contract.
 */
import { describe, test } from "node:test";
import { exportServicePack, parseServicePack } from "./servicePacks.ts";
import assert from "node:assert/strict";

describe("parseServicePack", () => {

  test("rejects non-object input", () => {

    const result = parseServicePack(null);

    assert.match(result.errors[0] ?? "", /expected a JSON object/);
  });

  test("rejects array input", () => {

    const result = parseServicePack([]);

    assert.match(result.errors[0] ?? "", /expected a JSON object/);
  });

  test("rejects primitive input", () => {

    assert.match((parseServicePack(42).errors[0] ?? ""), /expected a JSON object/);
    assert.match((parseServicePack("string").errors[0] ?? ""), /expected a JSON object/);
  });

  test("rejects missing name field", () => {

    const result = parseServicePack({ profiles: { p: { extends: "fullscreenApi" } }, version: 1 });

    assert.match(result.errors.join(" "), /Missing or empty 'name' field/);
  });

  test("rejects empty name field", () => {

    const result = parseServicePack({ name: "   ", profiles: { p: { extends: "fullscreenApi" } }, version: 1 });

    assert.match(result.errors.join(" "), /Missing or empty 'name' field/);
  });

  test("rejects missing version field", () => {

    const result = parseServicePack({ name: "test", profiles: { p: { extends: "fullscreenApi" } } });

    assert.match(result.errors.join(" "), /Missing or invalid 'version' field/);
  });

  test("rejects non-integer version field", () => {

    const result = parseServicePack({ name: "test", profiles: { p: { extends: "fullscreenApi" } }, version: 1.5 });

    assert.match(result.errors.join(" "), /must be an integer/);
  });

  test("rejects unsupported version (newer than CURRENT_VERSION)", () => {

    const result = parseServicePack({ name: "test", profiles: { p: { extends: "fullscreenApi" } }, version: 999 });

    assert.match(result.errors.join(" "), /Unsupported version/);
  });

  test("rejects missing profiles field", () => {

    const result = parseServicePack({ name: "test", version: 1 });

    assert.match(result.errors.join(" "), /Missing or empty 'profiles' field/);
  });

  test("rejects empty profiles object (must contain at least one)", () => {

    const result = parseServicePack({ name: "test", profiles: {}, version: 1 });

    assert.match(result.errors.join(" "), /at least one profile is required/);
  });

  test("accepts a minimal valid pack", () => {

    const result = parseServicePack({ name: "test", profiles: { p: { extends: "fullscreenApi" } }, version: 1 });

    assert.deepEqual(result.errors, []);
    assert.ok(result.pack);
    assert.equal(result.pack.name, "test");
    assert.equal(result.pack.version, 1);
    assert.ok(result.pack.profiles["p"]);
  });

  test("normalizes legacy noVideo flag to staticCapture before validation", () => {

    const result = parseServicePack({


      name: "test",
      profiles: {

        p: { extends: "fullscreenApi", noVideo: true } as Record<string, unknown>
      },
      version: 1
    });

    assert.deepEqual(result.errors, []);
    assert.equal((result.pack?.profiles["p"] as Record<string, unknown>)["staticCapture"], true);
    assert.equal("noVideo" in (result.pack?.profiles["p"] ?? {}), false);
  });

  test("includes domains in the pack when valid", () => {

    const result = parseServicePack({


      domains: {

        "custom-site.example": { profile: "p" }
      },
      name: "test",
      profiles: {

        p: { extends: "fullscreenApi" }
      },
      version: 1
    });

    assert.deepEqual(result.errors, []);
    assert.ok(result.pack?.domains?.["custom-site.example"]);
  });

  test("rejects channels missing required name and url", () => {

    const result = parseServicePack({


      channels: {

        "broken-ch": { other: "stuff" }
      },
      name: "test",
      profiles: {

        p: { extends: "fullscreenApi" }
      },
      version: 1
    });

    assert.match(result.errors.join(" "), /requires 'name' and 'url' fields/);
  });

  test("includes channels when each entry has name and url", () => {

    const result = parseServicePack({


      channels: {

        "my-channel": { name: "My Ch", url: "https://example.com/live" }
      },
      name: "test",
      profiles: {

        p: { extends: "fullscreenApi" }
      },
      version: 1
    });

    assert.deepEqual(result.errors, []);
    assert.ok(result.pack?.channels?.["my-channel"]);
  });

  test("rejects channels with non-object value entries (silently ignored - boundary)", () => {

    // Boundary: non-object channel entries are silently dropped, not reported as errors. The pack just won't have that channel.
    const result = parseServicePack({

      channels: {

        "non-object": "not-an-object",
        "valid-channel": { name: "OK", url: "https://example.com" }
      },
      name: "test",
      profiles: {

        p: { extends: "fullscreenApi" }
      },
      version: 1
    });

    assert.deepEqual(result.errors, []);

    const channels = result.pack?.channels;

    assert.ok(channels, "pack contains channels");
    assert.ok(channels["valid-channel"], "valid channel survives");
    assert.equal(channels["non-object"], undefined, "non-object value dropped");
  });

  test("rejects an array-shape channels value with the explicit 'expected an object' error", () => {

    /* Boundary: the channels validator at servicePacks.ts:117 explicitly rejects arrays via Array.isArray, not just non-object values. A pack with an array
     * for the channels field must surface the named error rather than silently iterate a numeric-keyed shape.
     */
    const result = parseServicePack({

      channels: [],
      name: "test",
      profiles: {

        p: { extends: "fullscreenApi" }
      },
      version: 1
    });

    assert.match(result.errors.join(" "), /Invalid 'channels' field/, "array-shape channels rejected with the named error");
  });

  test("sanitizes non-printable characters in channel string fields before storing", () => {

    /* The sanitization gate at servicePacks.ts:135-148 strips non-printable characters from string fields before adding the channel to the pack. Pinning the
     * gate explicitly catches a regression where a refactor stops calling sanitizeString on a particular field; without this assertion only the name/url path
     * is exercised, leaving channelSelector / stationId / profile untested. We construct each test value with String.fromCharCode so non-printable bytes are
     * literal in the source rather than invisible characters in editor buffers.
     */
    const NUL = String.fromCharCode(0);
    const SOH = String.fromCharCode(1);
    const result = parseServicePack({

      channels: {

        "sanitized-ch": {

          channelSelector: "ABC" + NUL + "DEF",
          name: "Sanitized" + SOH + "Channel",
          profile: "full" + NUL + "screenApi",
          stationId: "12" + SOH + "345",
          url: "https://example.com" + NUL + "/live"
        }
      },
      name: "test",
      profiles: {

        p: { extends: "fullscreenApi" }
      },
      version: 1
    });

    const channel: { channelSelector?: string; name?: string; profile?: string; stationId?: string; url?: string } = result.pack?.channels?.["sanitized-ch"] ?? {};

    assert.ok(result.pack, "pack returned without errors");
    assert.equal((channel.name ?? "").includes(SOH), false, "non-printable stripped from name");
    assert.equal((channel.url ?? "").includes(NUL), false, "non-printable stripped from url");
    assert.equal((channel.channelSelector ?? "").includes(NUL), false, "non-printable stripped from channelSelector");
    assert.equal((channel.stationId ?? "").includes(SOH), false, "non-printable stripped from stationId");
    assert.equal((channel.profile ?? "").includes(NUL), false, "non-printable stripped from profile");
  });

  test("propagates profile validation errors from validateImportedProfiles", () => {

    const result = parseServicePack({


      name: "test",
      profiles: {

        p: { extends: "not-a-real-profile" }
      },
      version: 1
    });

    assert.ok(result.errors.length > 0);
    assert.match(result.errors.join(" "), /non-existent built-in profile/);
  });
});

describe("exportServicePack", () => {

  test("returns null when none of the requested profiles exist in the user catalog", () => {

    // In unit tests no user profiles are loaded, so any key returns null.
    const result = exportServicePack(["nonexistent-profile-key"]);

    assert.equal(result, null);
  });

  test("returns null for an empty array of profile keys", () => {

    const result = exportServicePack([]);

    assert.equal(result, null);
  });
});
