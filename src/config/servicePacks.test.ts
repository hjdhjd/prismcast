/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * servicePacks.test.ts: Unit tests for the service-pack import/export module. The pure parseServicePack pipeline does heavy validation and sanitization;
 * tests cover the version gate, missing-field detection, channel sub-validation, and the legacy flag normalization that runs before profile validation.
 * exportServicePack is also exercised against the loaded user-profile state - which in unit tests is empty - so the test focuses on the null-return contract.
 */
import { countNewKeys, exportServicePack, parseServicePack } from "./servicePacks.ts";
import { describe, test } from "node:test";
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
    assert.ok(result.pack, "parseServicePack should return a pack on the success path");
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

  test("rejects a channel whose url is not http or https", () => {

    // The channel URL goes through the same shared check every other channel ingress uses, so a non-http(s) scheme is refused here rather than being stored and
    // failing later at tune time. The message is composed in this file's per-channel shape so it reads like every other channel error the pack can produce.
    const result = parseServicePack({


      channels: {

        "bad-scheme": { name: "Bad", url: "ftp://example.com/live" }
      },
      name: "test",
      profiles: {

        p: { extends: "fullscreenApi" }
      },
      version: 1
    });

    assert.match(result.errors.join(" "), /Channel 'bad-scheme': URL must use http or https protocol\./);
    assert.equal(result.pack, undefined, "a pack carrying an unusable channel URL yields no pack");
  });

  test("accepts a pack of ordinary https channels unchanged", () => {

    // The complement of the rejection case: adding the URL check must not narrow what already imports. Two well-formed channels, both kept, no errors.
    const result = parseServicePack({


      channels: {

        "ch-one": { name: "One", url: "https://example.com/one" },
        "ch-two": { name: "Two", url: "http://example.com/two" }
      },
      name: "test",
      profiles: {

        p: { extends: "fullscreenApi" }
      },
      version: 1
    });

    assert.deepEqual(result.errors, []);
    assert.ok(result.pack?.channels?.["ch-one"]);
    assert.ok(result.pack.channels["ch-two"]);
  });

  test("one unusable channel url rejects the whole pack, profiles and valid channels included", () => {

    /* The failure granularity is stated outright here because it differs from the sibling channel ingresses. errors is function-scoped and parseServicePack returns
     * only the errors once any are present, so a mixed-validity pack yields nothing at all - not a pack with the good channel and a warning about the bad one.
     * A service pack is a curated artifact imported as a unit, and this file already treats a missing name or url the same way.
     */
    const result = parseServicePack({


      channels: {

        "bad-ch": { name: "Bad", url: "javascript:alert(1)" },
        "good-ch": { name: "Good", url: "https://example.com/good" }
      },
      domains: {

        "custom-site.example": { profile: "p" }
      },
      name: "test",
      profiles: {

        p: { extends: "fullscreenApi" }
      },
      version: 1
    });

    assert.match(result.errors.join(" "), /Channel 'bad-ch': URL must use http or https protocol\./);
    assert.equal(result.pack, undefined, "no pack is produced, so the valid channel, the profile, and the domain are all discarded with it");
  });

  test("skipChannels leaves an unusable channel url unjudged and the pack importable", () => {

    /* The import route lets a caller take a pack's profiles and domains while ignoring its channels, and the people who reach for that are exactly the people
     * whose packs carry channels they do not trust. Judging a channel nobody is importing would block the profiles they came for, so the flag has to reach the
     * parse and not just the import.
     */
    const result = parseServicePack({


      channels: {

        "bad-scheme": { name: "Bad", url: "ftp://example.com/live" }
      },
      name: "test",
      profiles: {

        p: { extends: "fullscreenApi" }
      },
      version: 1
    }, { skipChannels: true });

    assert.deepEqual(result.errors, []);
    assert.ok(result.pack, "the pack survives because its channels were never judged");
    assert.equal(result.pack.channels, undefined, "a pack parsed with channels skipped carries none, so no unvalidated channel can be imported later");
    assert.ok(result.pack.profiles["p"], "the profiles the caller came for are present");
  });

  test("the same unusable channel url still rejects the whole pack without skipChannels", () => {

    // The complement of the case above, and the one that shows the flag narrowed nothing about the validation itself: the identical pack parsed without the
    // flag is still refused in full.
    const result = parseServicePack({


      channels: {

        "bad-scheme": { name: "Bad", url: "ftp://example.com/live" }
      },
      name: "test",
      profiles: {

        p: { extends: "fullscreenApi" }
      },
      version: 1
    });

    assert.match(result.errors.join(" "), /Channel 'bad-scheme': URL must use http or https protocol\./);
    assert.equal(result.pack, undefined, "without the flag the pack is still rejected in full");
  });

  test("skipChannels drops valid channels too while profiles and domains survive", () => {

    // Skipping is about what the caller asked to import, not about whether the channels were any good, so a pack of perfectly valid channels loses them just
    // the same. What must survive is everything the caller did ask for.
    const result = parseServicePack({


      channels: {

        "good-ch": { name: "Good", url: "https://example.com/good" }
      },
      domains: {

        "custom-site.example": { profile: "p" }
      },
      name: "test",
      profiles: {

        p: { extends: "fullscreenApi" }
      },
      version: 1
    }, { skipChannels: true });

    assert.deepEqual(result.errors, []);
    assert.equal(result.pack?.channels, undefined, "valid channels are dropped as well when the caller skips channels");
    assert.ok(result.pack?.domains?.["custom-site.example"], "domain mappings survive");
    assert.ok(result.pack.profiles["p"], "profiles survive");
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

    /* Boundary: the channels-field type guard in parseServicePack explicitly rejects arrays via Array.isArray, not just non-object values. A pack with an array
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

    /* The channel string-field sanitization loop in parseServicePack strips non-printable characters from string fields before adding the channel to the pack.
     * Asserting the gate explicitly catches a regression where a refactor stops calling sanitizeString on a particular field; without this assertion only the
     * name/url path is exercised, leaving channelSelector / stationId / profile untested. We construct each test value with String.fromCharCode so non-printable
     * bytes are literal in the source rather than invisible characters in editor buffers.
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
    assert.match(result.errors.join(" "), /non-existent builtin profile/);
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

describe("countNewKeys (net-new import accounting)", () => {

  /* countNewKeys is the pure kernel of importServicePack's summary counts. The stateful import orchestrator (which round-trips through the profiles and channels file
   * stores) is exercised at the integration tier, but the net-new arithmetic - counting only keys absent from the store so overwriting re-imports report zero
   * additions - is asserted here against the pure function so a regression to raw pack-size counting is caught at the unit tier.
   */

  test("counts every key as net-new when the existing record is empty", () => {

    assert.equal(countNewKeys({ a: 1, b: 2, c: 3 }, {}), 3);
  });

  test("reports zero net-new when the import fully overwrites pre-existing keys", () => {

    // This is the core regression guard: re-importing a pack whose keys already exist must report zero additions, not the full pack size. Differing values do not
    // matter - net-new is keyed on key presence, since Object.assign overwrites the value regardless.
    const existing = { a: "old-1", b: "old-2" };

    assert.equal(countNewKeys({ a: "new-1", b: "new-2" }, existing), 0);
  });

  test("counts only the keys absent from the existing record on a partial overlap", () => {

    // Two of the three incoming keys already exist; only the genuinely new key is counted.
    const existing = { a: 1, b: 2 };

    assert.equal(countNewKeys({ a: 9, b: 9, d: 9 }, existing), 1);
  });

  test("counts zero for an empty incoming record regardless of existing contents", () => {

    assert.equal(countNewKeys({}, { a: 1, b: 2 }), 0);
  });

  test("treats a prototype-named incoming key as net-new against an empty store (own-key membership)", () => {

    /* Boundary: countNewKeys uses Object.hasOwn, not the `in` operator, so it never treats inherited Object.prototype members as pre-existing. Service-pack keys are
     * user-controllable strings parsed from JSON, so a pack could legitimately carry a profile or channel keyed "constructor" or "toString". Against an empty store
     * such a key is genuinely net-new and must count as 1; the naive `in` operator would have falsely reported 0 because "constructor" in {} is true.
     */
    assert.equal(countNewKeys({ constructor: 1 }, {}), 1);
    assert.equal(countNewKeys({ toString: 1 }, {}), 1);
  });

  test("counts a prototype-named key as pre-existing only when it is a genuine own property of the store", () => {

    // The mirror of the boundary above: when the existing store really does contain a "constructor" entry as an own property, re-importing it is an overwrite, so it
    // is not net-new.
    const existing: Record<string, unknown> = {};

    Reflect.set(existing, "constructor", "already-here");

    assert.equal(countNewKeys({ constructor: 1 }, existing), 0);
  });
});
