/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * consistency-probe.test.ts: Integration coverage for the cross-store consistency probe, which reports what an operator must act on and changes nothing: the
 * dangling-variant-canonical and dangling-domain-profile checks, each surfacing a warning for the operator. The service filter's restriction to known tags is
 * the services module's own rule, covered in cross-store-consistency.test.ts.
 *
 * A report has no on-disk side effect to observe, so the suite captures LOG output. The probe logs through the process-wide LOG, whose entries also flow to the
 * SSE emitter before the console/file branch; we subscribe to that emitter (subscribeToLogs) and assert against the captured level and formatted message - the
 * same observable an operator sees on the Logs tab. Console logging defaults off under test, so the subscription is silent.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { createIntegrationContext, initializePersistence } from "../../helpers/integration.helpers.ts";
import type { LogEntry } from "../../../src/utils/logEmitter.ts";
import assert from "node:assert/strict";
import { mutateChannels } from "../../../src/config/userChannels.ts";
import { mutateProfiles } from "../../../src/config/userProfiles.ts";
import { runConsistencyProbeAtStartup } from "../../../src/config/consistencyProbe.ts";
import { subscribeToLogs } from "../../../src/utils/logEmitter.ts";

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
     * intentionally non-destructive - the right cleanup depends on operator intent (re-create the canonical vs. delete the variant), so the probe surfaces and waits.
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
