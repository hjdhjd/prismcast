/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * consistency-probe.test.ts: Integration coverage for the warn-only paths of the cross-store consistency probe and for the probe's auto-fix exception swallow.
 * The probe's auto-fix path (unknown-service-tag) is covered separately in cross-store-consistency.test.ts; this file owns the paths that emit a warning but
 * have no auto-fix - dangling-variant-canonical and dangling-domain-profile - plus the exception-swallow safety net inside runConsistencyProbeAtStartup.
 *
 * The warn-only paths have no on-disk side effect to observe (there is no auto-fix to land), so the suite captures LOG output. The probe logs through the
 * process-wide LOG, whose entries also flow to the SSE emitter before the console/file branch; we subscribe to that emitter (subscribeToLogs) and assert
 * against the captured level and formatted message - the same observable an operator sees on the Logs tab. Console logging defaults off under test, so the
 * subscription is silent.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { createIntegrationContext, initializePersistence, pathInDataDir } from "../../helpers/integration.helpers.ts";
import type { IntegrationContext } from "../../helpers/integration.helpers.ts";
import type { LogEntry } from "../../../src/utils/logEmitter.ts";
import assert from "node:assert/strict";
import { mutateChannels } from "../../../src/config/userChannels.ts";
import { mutateConfig } from "../../../src/config/userConfig.ts";
import { mutateProfiles } from "../../../src/config/userProfiles.ts";
import { runConsistencyProbeAtStartup } from "../../../src/config/consistencyProbe.ts";
import { setEnabledServices } from "../../../src/config/services.ts";
import { subscribeToLogs } from "../../../src/utils/logEmitter.ts";
import { writeFile } from "node:fs/promises";

// Every emitted log entry for the duration of a test. Populated by the subscribeToLogs subscription installed in beforeEach and reset per test so one test's
// probe output cannot leak into another's assertions. Filtered by level and message substring the same way an operator would scan the Logs tab.
let captured: LogEntry[];

let unsubscribe: () => void;

beforeEach(() => {

  captured = [];
  unsubscribe = subscribeToLogs((entry) => { captured.push(entry); });
});

afterEach(() => {

  unsubscribe();
});

describe("consistency probe - dangling-variant-canonical detection", () => {

  test("a stored variant whose canonicalKey points at a missing channel surfaces a warn-level issue without mutating state", async () => {

    /* The check walks every entry in getStoredUserChannels() and, for each one carrying a canonicalKey, verifies the referenced canonical exists in
     * PREDEFINED_CHANNELS or the user's stored map. Variants that point at nothing surface as a "dangling-variant-canonical" warn issue. The check is
     * intentionally non-destructive (no autoFix) - the right cleanup depends on operator intent (re-create the canonical vs. delete the variant), so the
     * probe surfaces and waits.
     *
     * The seed: a stored variant with a canonicalKey that does not exist in either source. We use a randomized variant key plus a clearly-not-real canonical
     * so the test cannot accidentally collide with a future predefined entry.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Seed: a variant entry whose canonicalKey references a non-existent canonical. mutateChannels' normalizer classifies this as a variant (key !==
    // canonicalKey AND canonicalKey is set) and preserves the canonicalKey field even though the canonical is missing - the dangling-canonical fallback at
    // userChannels.ts retains the variant as best it can. The probe then walks the stored map and surfaces the issue.
    await mutateChannels((data) => {

      data.channels["fake-variant-x9z2"] = { canonicalKey: "definitely-missing-canonical-y7a3", url: "https://example.test/fake" };
    });

    await runConsistencyProbeAtStartup();

    // Assert: a warn line carries the dangling-variant-canonical category, names the variant, and names the missing canonical.
    const matching = captured.filter((line) => {

      return (line.level === "warn") && line.message.includes("dangling-variant-canonical") && line.message.includes("fake-variant-x9z2");
    });

    assert.ok(matching.length >= 1, "at least one warn-level line names the dangling variant and the missing canonical");
    assert.match(matching[0]?.message ?? "", /definitely-missing-canonical-y7a3/, "the missing canonical key is included for operator triage");

    // No error-level lines for this category - dangling canonicals are warn-only by design.
    const errors = captured.filter((line) => (line.level === "error") && line.message.includes("dangling-variant-canonical"));

    assert.equal(errors.length, 0, "dangling-variant-canonical is warn-only; the probe must not escalate to error severity");
  });
});

describe("consistency probe - dangling-domain-profile detection", () => {

  test("a user domain mapping pointing at a missing profile surfaces a warn-level issue", async () => {

    /* The check walks getUserDomains() and, for each domain carrying a profile reference, resolves it against both getBuiltinProfile() and the user-defined
     * profile store (getUserProfiles). A domain surfaces as dangling only when its profile exists in neither table, so the test mapping points at a key that
     * exists in no profile table at all.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Seed: a user domain mapping whose profile references a non-existent profile. mutateProfiles persists the data unchanged - it does not validate cross-
    // store profile references because that is the consistency probe's job.
    await mutateProfiles((data) => {

      data.domains["test-domain-q4r1.example"] = { profile: "missing-profile-p5s8" };
    });

    await runConsistencyProbeAtStartup();

    const matching = captured.filter((line) => {

      return (line.level === "warn") && line.message.includes("dangling-domain-profile") && line.message.includes("test-domain-q4r1.example");
    });

    assert.ok(matching.length >= 1, "at least one warn-level line names the dangling domain mapping and the missing profile");
    assert.match(matching[0]?.message ?? "", /missing-profile-p5s8/, "the missing profile key is included for operator triage");

    const errors = captured.filter((line) => (line.level === "error") && line.message.includes("dangling-domain-profile"));

    assert.equal(errors.length, 0, "dangling-domain-profile is warn-only by design");
  });

  test("a user domain mapping pointing at a user-defined profile is NOT flagged as dangling", async () => {

    // Regression guard for the dual-table lookup: checkDomainProfiles resolves each domain's profile against builtin profiles AND the user-defined profile store
    // (getUserProfiles), so mapping a domain onto a profile the user created is a valid configuration the save-path validator accepts and the probe must not warn
    // about. Consulting only the builtin table would surface this valid custom configuration as a false "dangling-domain-profile", which is exactly the outcome this
    // guard asserts against.
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Seed a user-defined profile and a domain mapping that points at it - both land in the user profile store, exactly what building a custom profile produces.
    await mutateProfiles((data) => {

      data.profiles["custom-profile-w7x2"] = { description: "user-defined regression profile" };
      data.domains["test-domain-w7x2.example"] = { profile: "custom-profile-w7x2" };
    });

    await runConsistencyProbeAtStartup();

    const danglingForOurDomain = captured.filter((line) => {

      return line.message.includes("dangling-domain-profile") && line.message.includes("test-domain-w7x2.example");
    });

    assert.equal(danglingForOurDomain.length, 0, "a domain mapped to a user-defined profile must not surface as a dangling reference");
  });
});

describe("consistency probe - auto-fix exception swallow", () => {

  test("a failing auto-fix logs a warning and does not propagate as an unhandled rejection", async () => {

    /* The probe runs every eligible auto-fix in parallel via Promise.allSettled, then inspects each settlement and logs a warn for any rejected result. This is
     * the safety net that prevents one broken auto-fix from crashing the startup probe. We force a real auto-fix to fail by:
     *
     *   1. Setting up the unknown-service-tag scenario so checkServiceTagFilter surfaces an issue with an auto-fix that calls mutateEnabledServices (which
     *      goes through mutateConfig, which goes through the file store).
     *   2. Corrupting config.json AND config.json.bak between initializePersistence and the probe call so the file store's read fails parse on both files;
     *      the framework throws FileStoreParseError from within the auto-fix's mutate path.
     *   3. Asserting the probe still resolves cleanly (no unhandled rejection), the rejected-settlement branch's "Consistency probe auto-fix failed" warn line
     *      landed, and the probe identified the failing category.
     *
     * If the rejection were not inspected and logged, the FileStoreParseError would surface as a process-level unhandled rejection (Node's --test runner would
     * treat it as a test failure or even abort the run). Inspecting each settlement keeps the rest of startup safe.
     */
    await using ctx: IntegrationContext = await createIntegrationContext();

    await initializePersistence(ctx);

    // Seed the unknown-service-tag scenario: enabledServices contains a fake tag that no service offers. The probe's checkServiceTagFilter will surface a
    // warning issue whose autoFix is mutateEnabledServices.
    setEnabledServices([ "hulu", "totally-fake-tag-q8r2" ]);
    await mutateConfig((config) => {

      config.channels ??= {};
      config.channels.enabledServices = [ "hulu", "totally-fake-tag-q8r2" ];
    });

    // Now corrupt config.json and config.json.bak so that the autoFix's mutateConfig call hits the corruption guard. mutateConfig calls the file store's
    // mutate -> read; read attempts the corrupt main, fails parse, attempts .bak, fails parse on .bak too, and surfaces parseError. mutate then throws
    // FileStoreParseError, which rejects the autoFix call - the rejection surfaces as a rejected settlement that the probe inspects and logs.
    await writeFile(pathInDataDir(ctx, "config.json"), "{ this is not valid json", "utf-8");
    await writeFile(pathInDataDir(ctx, "config.json.bak"), "{ neither is this", "utf-8");

    // The probe must complete without rejecting - the per-settlement rejection inspection over Promise.allSettled logs the failure and lets startup proceed.
    await assert.doesNotReject(() => runConsistencyProbeAtStartup(),
      "the probe must complete cleanly even when an autoFix throws - the catch is the safety net for partial-progress startup");

    // The rejected-settlement branch surfaces the failed category in the operator-visible log line for triage. The message shape is "Consistency probe auto-fix
    // failed for <category>: <reason>."; we assert the category appears in that formatted line - the same text an operator reads on the Logs tab.
    const swallowed = captured.filter((line) => (line.level === "warn") && line.message.includes("Consistency probe auto-fix failed"));

    assert.ok(swallowed.length >= 1, "the swallowed autoFix failure is logged at warn level");
    assert.ok(swallowed[0]?.message.includes("unknown-service-tag"),
      "the failing category surfaces in the log line so operators can identify which auto-fix broke");
  });
});
