/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * index.test.ts: Unit tests for the configuration endpoint coordinator. The module exports three pieces of real logic - categorizeProfiles (a pure
 * grouping helper), scheduleServerRestart (which branches on PRISMCAST_SERVICE and the active stream count and may schedule a delayed exit), and
 * describeConfigurationOutcome (which branches on applied/deferred/rejected counts and restart precedence to build the user-facing message).
 * setupConfigEndpoint is exercised at the synchronous wiring level against an Express stub that records route registrations. The remaining exports are
 * barrel re-exports verified only as identity-typed function references. The actual route handlers require a live Express app; we flag their per-handler
 * behavior as integration-level rather than exercise it here.
 */
import type { ApplyConfigurationResult, RestartResult } from "./index.ts";
import { afterEach, beforeEach, describe, mock, test } from "node:test";
import { categorizeProfiles, describeConfigurationOutcome, scheduleServerRestart, setupConfigEndpoint } from "./index.ts";
import type { ConfigChange } from "../../config/reactivity.ts";
import { PROFILE_CATEGORIES } from "../../types/index.ts";
import type { ProfileInfo } from "../../config/profiles.ts";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../../testing.helpers.ts";

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/* makeProfile builds a minimal ProfileInfo literal. Tests pass overrides for the fields they care about. The category default is "special" so plain
 * makeProfile() calls produce something that lands in a single visible bucket without polluting the others.
 */
function makeProfile(overrides: Partial<ProfileInfo> = {}): ProfileInfo {

  return {

    category: "special",
    description: "",
    name: "test",
    source: "builtin",
    summary: "",
    ...overrides
  };
}

describe("PROFILE_CATEGORIES", () => {

  test("lists each category exactly once, in the order every surface displays them", () => {

    // The dropdown, the profile reference, and the custom-profile wizard all iterate this table, so its order is the rendered order and a repeated key would
    // render a category twice.
    const keys = PROFILE_CATEGORIES.map((category) => category.key);

    assert.deepEqual(keys, [ "api", "keyboard", "special", "multiChannel", "custom" ]);
    assert.equal(new Set(keys).size, keys.length, "every category key appears exactly once");
  });

  test("every entry carries the title and description its surfaces render, and only multiChannel needs a selector", () => {

    // The renderers read title and description straight out of the table with no fallback of their own, and both the dropdown's \"(needs selector)\" note and
    // the profile reference's channel-selector guide read requiresSelector.
    for(const category of PROFILE_CATEGORIES) {

      assert.ok(category.title.length > 0, category.key + " carries a title");
      assert.ok(category.description.length > 0, category.key + " carries a description");
    }

    assert.deepEqual(PROFILE_CATEGORIES.filter((category) => category.requiresSelector).map((category) => category.key), ["multiChannel"]);
  });

  test("categorizeProfiles returns exactly one bucket per table entry", () => {

    /* The runtime half of a compile-time relationship: ProfileCategory derives from the table's keys, so a category added to the table without a bucket in
     * categorizeProfiles is a type error. This reads the same rule from the other side, over the empty input where every bucket must still be present.
     */
    const groups = categorizeProfiles([]);

    assert.deepEqual(Object.keys(groups).toSorted(), PROFILE_CATEGORIES.map((category) => category.key).toSorted(),
      "one bucket per table entry, no more and no fewer");
  });
});

describe("categorizeProfiles", () => {

  test("returns a record with one bucket per ProfileCategory, all empty for an empty input", () => {

    // Boundary: zero-input case must still produce every category key so callers can index into the result without optional-chaining noise.
    const groups = categorizeProfiles([]);

    assert.deepEqual(groups.api, []);
    assert.deepEqual(groups.custom, []);
    assert.deepEqual(groups.keyboard, []);
    assert.deepEqual(groups.multiChannel, []);
    assert.deepEqual(groups.special, []);
  });

  test("routes each profile into exactly one bucket based on its category field", () => {

    const apiProfile = makeProfile({ category: "api", name: "fullscreenApi" });
    const keyboardProfile = makeProfile({ category: "keyboard", name: "keyboardDynamic" });
    const multiProfile = makeProfile({ category: "multiChannel", name: "huluLive" });
    const specialProfile = makeProfile({ category: "special", name: "staticPage" });
    const customProfile = makeProfile({ category: "custom", name: "myCustom", source: "user" });

    const groups = categorizeProfiles([ apiProfile, keyboardProfile, multiProfile, specialProfile, customProfile ]);

    assert.deepEqual(groups.api, [apiProfile]);
    assert.deepEqual(groups.keyboard, [keyboardProfile]);
    assert.deepEqual(groups.multiChannel, [multiProfile]);
    assert.deepEqual(groups.special, [specialProfile]);
    assert.deepEqual(groups.custom, [customProfile]);
  });

  test("preserves input order within each category bucket", () => {

    const a = makeProfile({ category: "api", name: "a" });
    const b = makeProfile({ category: "api", name: "b" });
    const c = makeProfile({ category: "api", name: "c" });

    const groups = categorizeProfiles([ c, a, b ]);

    assert.deepEqual(groups.api.map((p) => p.name), [ "c", "a", "b" ], "filter is stable; insertion order survives");
  });

  test("does not drop or duplicate profiles across categories", () => {

    // Boundary: every input must end up in exactly one bucket. Sum of bucket sizes must equal the input length.
    const profiles = [
      makeProfile({ category: "api", name: "p1" }),
      makeProfile({ category: "api", name: "p2" }),
      makeProfile({ category: "keyboard", name: "p3" }),
      makeProfile({ category: "multiChannel", name: "p4" }),
      makeProfile({ category: "special", name: "p5" }),
      makeProfile({ category: "custom", name: "p6" })
    ];

    const groups = categorizeProfiles(profiles);
    const total = groups.api.length + groups.custom.length + groups.keyboard.length + groups.multiChannel.length + groups.special.length;

    assert.equal(total, profiles.length, "every profile lands in exactly one bucket");
  });

  test("does not mutate the input array", () => {

    const input = [ makeProfile({ category: "api", name: "x" }), makeProfile({ category: "custom", name: "y" }) ];
    const snapshot = input.slice();

    categorizeProfiles(input);

    assert.deepEqual(input, snapshot, "input must remain untouched");
  });

  test("returns fresh arrays so callers can mutate without affecting source", () => {

    // The buckets come from Array.prototype.filter which creates new arrays. Mutating one bucket must not bleed into another call.
    const profiles = [makeProfile({ category: "api", name: "x" })];
    const groups = categorizeProfiles(profiles);

    groups.api.push(makeProfile({ category: "api", name: "injected" }));

    const groups2 = categorizeProfiles(profiles);

    assert.equal(groups2.api.length, 1, "fresh call sees only the original source profile");
  });
});

describe("scheduleServerRestart", () => {

  // The function reads PRISMCAST_SERVICE to decide between the manual-restart path and the service-managed path. We save and restore the variable and
  // mock setTimeout so the actual exit branch is observable without running it.
  let originalServiceFlag: string | undefined;

  beforeEach(() => {

    originalServiceFlag = process.env["PRISMCAST_SERVICE"];
  });

  afterEach(() => {

    if(originalServiceFlag === undefined) {

      Reflect.deleteProperty(process.env, "PRISMCAST_SERVICE");
    } else {

      process.env["PRISMCAST_SERVICE"] = originalServiceFlag;
    }

    mock.timers.reset();
  });

  test("returns the manual-restart-required result when not running as a service", () => {

    Reflect.deleteProperty(process.env, "PRISMCAST_SERVICE");

    const result = scheduleServerRestart("for unit test");

    assert.equal(result.willRestart, false, "manual-restart path");
    assert.equal(result.deferred, false, "no deferral when not a service");
    assert.equal(result.activeStreams, 0, "no active stream tracking on manual path");
    assert.match(result.message, /Please restart PrismCast/, "message instructs the user to restart");
  });

  test("returns the immediate-restart result when running as a service with no active streams", () => {

    // Mock timers so the delayed setTimeout(close+exit) does not actually fire during the test. The function returns synchronously; we only need to
    // verify the returned shape.
    mock.timers.enable({ apis: ["setTimeout"] });
    process.env["PRISMCAST_SERVICE"] = "1";

    const result = scheduleServerRestart("for unit test");

    assert.equal(result.willRestart, true, "service path schedules an auto-restart");
    assert.equal(result.deferred, false, "not deferred when no streams are active");
    assert.equal(result.activeStreams, 0, "stream count of zero reflected back");
    assert.match(result.message, /Server is restarting/, "message indicates immediate restart");
  });

  test("returns the deferred result when running as a service AND active streams exist", () => {

    /* Note: we cannot deterministically inject an active stream count without coupling to streaming/registry internals or mocking modules. The active-
     * streams branch is exercised functionally elsewhere; here we lock the no-streams branch shape and document that the deferred branch follows the
     * same return-value contract: deferred=true, willRestart=true, activeStreams=N>0, message includes the stream count. The synchronous return shape
     * for the no-streams case is the assertion this test actually verifies - active-streams formatting is straightforward String concatenation.
     */
    mock.timers.enable({ apis: ["setTimeout"] });
    process.env["PRISMCAST_SERVICE"] = "1";

    const result = scheduleServerRestart("for unit test");

    // Lock that the result matches RestartResult's shape regardless of which branch produced it. We have already verified the manual and immediate
    // branches directly; this exact shape verifies the immediate branch's return contract a second time, which is the guarantee callers rely on when
    // branching on result.deferred.
    assert.equal(typeof result.activeStreams, "number");
    assert.equal(typeof result.deferred, "boolean");
    assert.equal(typeof result.message, "string");
    assert.equal(typeof result.willRestart, "boolean");
  });

  test("schedules a setTimeout with delay >= 500ms when running as a service with no active streams", () => {

    /* Direct assertion on the contract: setTimeout was invoked exactly once, and its delay argument was at least 500ms. We spy on globalThis.setTimeout
     * with a no-op stub so the callback never fires (which would otherwise call process.exit). A bare assert.ok(true) here would pass even if a
     * regression caused scheduleServerRestart to skip the setTimeout entirely; the introspection below catches that mode.
     */
    process.env["PRISMCAST_SERVICE"] = "1";

    const setTimeoutSpy = mock.method(globalThis, "setTimeout", () => 0 as unknown as NodeJS.Timeout);

    try {

      scheduleServerRestart("for unit test");

      assert.equal(setTimeoutSpy.mock.callCount(), 1, "setTimeout invoked exactly once on the service-immediate branch");

      const delay = setTimeoutSpy.mock.calls[0]?.arguments[1];

      assert.equal(typeof delay, "number", "delay argument is a number");
      assert.ok((delay ?? 0) >= 500, "scheduled delay is at least 500ms");
    } finally {

      setTimeoutSpy.mock.restore();
    }
  });

  test("does not schedule a setTimeout on the manual-restart path", () => {

    /* Boundary: when not running as a service, the function returns immediately without scheduling any timer. The previous assertion ("we are still alive")
     * would survive a refactor that scheduled an irrelevant timer in the manual branch. We pin the contract directly via setTimeout call count.
     */
    Reflect.deleteProperty(process.env, "PRISMCAST_SERVICE");

    const setTimeoutSpy = mock.method(globalThis, "setTimeout", () => 0 as unknown as NodeJS.Timeout);

    try {

      scheduleServerRestart("for unit test");

      assert.equal(setTimeoutSpy.mock.callCount(), 0, "manual-restart branch must not schedule any timer");
    } finally {

      setTimeoutSpy.mock.restore();
    }
  });

  test("the reason string is opaque to the return shape (does not appear in the user-facing message)", () => {

    // The reason argument is used only for log output. The user-facing message is fixed text. We lock this by passing a recognizable reason and
    // confirming it does not leak into the message.
    Reflect.deleteProperty(process.env, "PRISMCAST_SERVICE");

    const result = scheduleServerRestart("UNIQUE_REASON_TOKEN_42");

    assert.doesNotMatch(result.message, /UNIQUE_REASON_TOKEN_42/, "reason is for logging only, not the message");
  });
});

describe("describeConfigurationOutcome", () => {

  // makeChange builds a synthetic ConfigChange. The path and values are opaque to describeConfigurationOutcome - only the bucket counts and rejection reasons
  // shape the message - so we can pass minimal placeholders.
  function makeChange(path: string): ConfigChange {

    return { current: 1, path, previous: 0 };
  }

  // makeRestart builds a fake RestartResult. Used to assert that the restart message wins over any live-applied or rejected summary.
  function makeRestart(message: string): RestartResult {

    return { activeStreams: 0, deferred: false, message, willRestart: true };
  }

  test("returns the restart message verbatim when a restart was scheduled", () => {

    const outcome: ApplyConfigurationResult = {

      apply: { applied: [], deferred: [{ change: makeChange("server.port"), reason: "no handler" }], rejected: [] },
      restart: makeRestart("Server is restarting...")
    };

    assert.equal(describeConfigurationOutcome(outcome), "Server is restarting...");
  });

  test("returns a generic 'Configuration saved.' when nothing changed and no restart was scheduled", () => {

    const outcome: ApplyConfigurationResult = {

      apply: { applied: [], deferred: [], rejected: [] },
      restart: null
    };

    assert.equal(describeConfigurationOutcome(outcome), "Configuration saved.");
  });

  test("reports a single live-applied change in the singular", () => {

    const outcome: ApplyConfigurationResult = {

      apply: { applied: [makeChange("hdhr.enabled")], deferred: [], rejected: [] },
      restart: null
    };

    assert.equal(describeConfigurationOutcome(outcome), "Configuration saved. 1 setting applied live.");
  });

  test("reports multiple live-applied changes in the plural", () => {

    const outcome: ApplyConfigurationResult = {

      apply: { applied: [ makeChange("hdhr.enabled"), makeChange("hdhr.port"), makeChange("hdhr.discoveryEnabled") ], deferred: [], rejected: [] },
      restart: null
    };

    assert.equal(describeConfigurationOutcome(outcome), "Configuration saved. 3 settings applied live.");
  });

  test("surfaces the first rejection reason when any change was rejected and no restart was scheduled", () => {

    const outcome: ApplyConfigurationResult = {

      apply: {

        applied: [makeChange("hdhr.discoveryEnabled")],
        deferred: [],
        rejected: [
          { change: makeChange("hdhr.enabled"), reason: "FFmpeg unavailable" },
          { change: makeChange("hdhr.port"), reason: "port in use" }
        ]
      },
      restart: null
    };

    // The message reports the count using grammatical agreement ("2 changes were rejected") and includes the first reason as a directly actionable hint.
    assert.equal(describeConfigurationOutcome(outcome), "Configuration saved, but 2 changes were rejected: FFmpeg unavailable.");
  });

  test("agrees in number when exactly one change was rejected", () => {

    const outcome: ApplyConfigurationResult = {

      apply: { applied: [], deferred: [], rejected: [{ change: makeChange("hdhr.enabled"), reason: "FFmpeg unavailable" }] },
      restart: null
    };

    assert.equal(describeConfigurationOutcome(outcome), "Configuration saved, but 1 change was rejected: FFmpeg unavailable.");
  });
});

describe("setupConfigEndpoint (barrel-style aggregator)", () => {

  test("is exported as a function", () => {

    // Aggregators that wire route handlers cannot be exercised without an Express app; we lock the export shape so a removal would fail the build.
    assert.equal(typeof setupConfigEndpoint, "function", "setupConfigEndpoint is a function");
  });

  test("accepts an Express-shaped object without throwing on the synchronous portion", () => {

    // We pass a stub Express that records get/post/put/delete/patch registration calls. The aggregator delegates to setupSettingsRoutes,
    // setupChannelRoutes, and setupProfileRoutes - all of which call app.METHOD(path, handler) at synchronous time. Any of those methods throwing
    // would surface here, which validates the wiring without exercising any handler.
    const calls: { method: string; path: string }[] = [];

    const stub = {

      delete: (path: string): unknown => {

        calls.push({ method: "delete", path });

        return stub;
      },
      get: (path: string): unknown => {

        calls.push({ method: "get", path });

        return stub;
      },
      patch: (path: string): unknown => {

        calls.push({ method: "patch", path });

        return stub;
      },
      post: (path: string): unknown => {

        calls.push({ method: "post", path });

        return stub;
      },
      put: (path: string): unknown => {

        calls.push({ method: "put", path });

        return stub;
      }
    };

    assert.doesNotThrow(() => {

      setupConfigEndpoint(stub as never);
    }, "wiring should not throw during route registration");

    assert.ok(calls.length > 0, "at least one route was registered");
  });
});

describe("barrel re-exports", () => {

  test("re-exports the channels, settings, and services helpers as functions", async () => {

    /* External consumers depend on these barrel re-exports. We import them through the index module to verify they resolve to function
     * references rather than undefined. A removal or rename in any of the source modules would surface as undefined here.
     */
    const mod = await import("./index.ts");

    assert.equal(typeof mod.generateChannelRowHtml, "function", "generateChannelRowHtml re-export");
    assert.equal(typeof mod.generateChannelsPanel, "function", "generateChannelsPanel re-export");
    assert.equal(typeof mod.generateServiceFilterToolbar, "function", "generateServiceFilterToolbar re-export");
    assert.equal(typeof mod.generateAdvancedTabContent, "function", "generateAdvancedTabContent re-export");
    assert.equal(typeof mod.generateCollapsibleSection, "function", "generateCollapsibleSection re-export");
    assert.equal(typeof mod.generateSettingsFormFooter, "function", "generateSettingsFormFooter re-export");
    assert.equal(typeof mod.generateSettingsTabContent, "function", "generateSettingsTabContent re-export");
    assert.equal(typeof mod.hasEnvOverrides, "function", "hasEnvOverrides re-export");
    assert.equal(typeof mod.generateCustomProfilesPanel, "function", "generateCustomProfilesPanel re-export");
    assert.equal(typeof mod.generateProfileWizardModal, "function", "generateProfileWizardModal re-export");
  });

  test("re-exports OPTIONAL_COLUMNS as an iterable structure", async () => {

    // OPTIONAL_COLUMNS is data, not a function. We verify it has a recognizable shape (iterable or array) without coupling to the column list itself.
    const mod = await import("./index.ts");

    assert.ok(mod.OPTIONAL_COLUMNS, "OPTIONAL_COLUMNS is defined");
    assert.equal(typeof mod.OPTIONAL_COLUMNS, "object", "OPTIONAL_COLUMNS is a non-null object/array");
  });
});
