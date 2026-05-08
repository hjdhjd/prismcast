/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * consistency-probe.test.ts: Integration coverage for the warn-only paths of the cross-store consistency probe and for the probe's auto-fix exception swallow.
 * The probe's auto-fix path (unknown-service-tag) is covered separately in cross-store-consistency.test.ts; this file owns the paths that emit a warning but
 * have no auto-fix - dangling-variant-canonical and dangling-domain-profile - plus the exception-swallow safety net inside runConsistencyProbeAtStartup.
 *
 * The warn-only paths cannot be observed via on-disk side effects (there is no auto-fix to land), so the suite must capture LOG output. We mock-module the
 * utils barrel with a proxy LOG that forwards to a per-test capturer, then dynamically import the modules under test so their static LOG bindings resolve
 * to the mock. The pattern follows persistence.integrity.test.ts and the precedents in src/routes/health.test.ts and test/e2e/streaming/pretune.test.ts.
 */
import type * as ConsistencyProbeModule from "../../../src/config/consistencyProbe.ts";
import type * as IntegrationHelpers from "../../helpers/integration.helpers.ts";
import type * as Services from "../../../src/config/services.ts";
import type * as UserChannels from "../../../src/config/userChannels.ts";
import type * as UserConfig from "../../../src/config/userConfig.ts";
import type * as UserProfiles from "../../../src/config/userProfiles.ts";
import { type CapturedLogLine, type TestLogger, capturingLog, silentLog } from "../../../src/testing.helpers.ts";
import { before, beforeEach, describe, mock, test } from "node:test";
import type { IntegrationContext } from "../../helpers/integration.helpers.ts";
import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";

// Per-test active logger; the proxy LOG installed via mock.module forwards every call here. Tests assign their own capturingLog before triggering the probe.
let activeLogger: TestLogger = silentLog();

// Lazily-bound modules. Populated in `before` after mock.module installs the LOG proxy.
let createIntegrationContext: typeof IntegrationHelpers.createIntegrationContext;
let initializePersistence: typeof IntegrationHelpers.initializePersistence;
let pathInDataDir: typeof IntegrationHelpers.pathInDataDir;
let mutateChannels: typeof UserChannels.mutateChannels;
let mutateProfiles: typeof UserProfiles.mutateProfiles;
let mutateConfig: typeof UserConfig.mutateConfig;
let setEnabledServices: typeof Services.setEnabledServices;
let runConsistencyProbeAtStartup: typeof ConsistencyProbeModule.runConsistencyProbeAtStartup;

before(async () => {

  // Pass-through every export of the utils barrel that we do not override, so the dynamically-imported modules under test can still resolve stringifySorted,
  // formatError, and the rest of the surface.
  const realUtils = await import("../../../src/utils/index.ts");

  const proxyLog: TestLogger = {

    debug: (category: string, message: string, ...args: unknown[]): void => { activeLogger.debug(category, message, ...args); },
    error: (message: string, ...args: unknown[]): void => { activeLogger.error(message, ...args); },
    info: (message: string, ...args: unknown[]): void => { activeLogger.info(message, ...args); },
    warn: (message: string, ...args: unknown[]): void => { activeLogger.warn(message, ...args); },
    withStreamId: (streamId: string) => activeLogger.withStreamId(streamId)
  };

  mock.module(new URL("../../../src/utils/index.ts", import.meta.url).href, {

    namedExports: { ...realUtils, LOG: proxyLog }
  });

  // Dynamic-import the modules under test so their static `import { LOG } from "../utils/index.ts"` bindings resolve to the proxy. The integration helper's
  // bootstrapping (createIntegrationContext, initializePersistence) and the probe entrypoint must all flow through this re-import, otherwise the persistence
  // store loading inside initialize* still emits to the real LOG and the test's capturer is silent.
  const integration = await import("../../helpers/integration.helpers.ts");

  createIntegrationContext = integration.createIntegrationContext;
  initializePersistence = integration.initializePersistence;
  pathInDataDir = integration.pathInDataDir;

  const userChannels = await import("../../../src/config/userChannels.ts");

  mutateChannels = userChannels.mutateChannels;

  const userProfiles = await import("../../../src/config/userProfiles.ts");

  mutateProfiles = userProfiles.mutateProfiles;

  const userConfig = await import("../../../src/config/userConfig.ts");

  mutateConfig = userConfig.mutateConfig;

  const services = await import("../../../src/config/services.ts");

  setEnabledServices = services.setEnabledServices;

  const consistencyProbe = await import("../../../src/config/consistencyProbe.ts");

  runConsistencyProbeAtStartup = consistencyProbe.runConsistencyProbeAtStartup;
});

beforeEach(() => {

  activeLogger = silentLog();
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
    const { logger, lines } = capturingLog();

    activeLogger = logger;

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
    const matching = lines().filter((line: CapturedLogLine) => {

      return (line.level === "warn") && line.message.includes("dangling-variant-canonical") && line.message.includes("fake-variant-x9z2");
    });

    assert.ok(matching.length >= 1, "at least one warn-level line names the dangling variant and the missing canonical");
    assert.match(matching[0]?.message ?? "", /definitely-missing-canonical-y7a3/, "the missing canonical key is included for operator triage");

    // No error-level lines for this category - dangling canonicals are warn-only by design.
    const errors = lines().filter((line: CapturedLogLine) => (line.level === "error") && line.message.includes("dangling-variant-canonical"));

    assert.equal(errors.length, 0, "dangling-variant-canonical is warn-only; the probe must not escalate to error severity");
  });
});

describe("consistency probe - dangling-domain-profile detection", () => {

  test("a user domain mapping pointing at a missing profile surfaces a warn-level issue", async () => {

    /* The check walks getUserDomains() and, for each domain carrying a profile reference, verifies the profile exists as a built-in. (User-defined profiles
     * are accepted via the same registry, but built-in resolution is the relevant path here - the test mapping points at a profile that exists in neither.)
     */
    const { logger, lines } = capturingLog();

    activeLogger = logger;

    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Seed: a user domain mapping whose profile references a non-existent profile. mutateProfiles persists the data unchanged - it does not validate cross-
    // store profile references because that is the consistency probe's job.
    await mutateProfiles((data) => {

      data.domains["test-domain-q4r1.example"] = { profile: "missing-profile-p5s8" };
    });

    await runConsistencyProbeAtStartup();

    const matching = lines().filter((line: CapturedLogLine) => {

      return (line.level === "warn") && line.message.includes("dangling-domain-profile") && line.message.includes("test-domain-q4r1.example");
    });

    assert.ok(matching.length >= 1, "at least one warn-level line names the dangling domain mapping and the missing profile");
    assert.match(matching[0]?.message ?? "", /missing-profile-p5s8/, "the missing profile key is included for operator triage");

    const errors = lines().filter((line: CapturedLogLine) => (line.level === "error") && line.message.includes("dangling-domain-profile"));

    assert.equal(errors.length, 0, "dangling-domain-profile is warn-only by design");
  });
});

describe("consistency probe - auto-fix exception swallow", () => {

  test("a failing auto-fix logs a warning and does not propagate as an unhandled rejection", async () => {

    /* The probe's auto-fix loop wraps each invocation in a try/catch that logs a warn and continues. This is the safety net that prevents one broken auto-fix
     * from crashing the startup probe. We force a real auto-fix to fail by:
     *
     *   1. Setting up the unknown-service-tag scenario so checkServiceTagFilter surfaces an issue with an auto-fix that calls mutateEnabledServices (which
     *      goes through mutateConfig, which goes through the file store).
     *   2. Corrupting config.json AND config.json.bak between initializePersistence and the probe call so the file store's read fails parse on both files;
     *      the framework throws FileStoreParseError from within the auto-fix's mutate path.
     *   3. Asserting the probe still resolves cleanly (no unhandled rejection), the catch path's "Consistency probe auto-fix failed" warn line landed, and
     *      the probe identified the failing category.
     *
     * If the catch were missing, the unhandled rejection would surface the FileStoreParseError as a process-level rejection (Node's --test runner would treat
     * it as a test failure or even abort the run). The catch keeps the rest of startup safe.
     */
    const { logger, lines } = capturingLog();

    activeLogger = logger;

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
    // FileStoreParseError, which propagates back into the autoFix call - which the probe's try/catch catches and logs.
    await writeFile(pathInDataDir(ctx, "config.json"), "{ this is not valid json", "utf-8");
    await writeFile(pathInDataDir(ctx, "config.json.bak"), "{ neither is this", "utf-8");

    // The probe must complete without rejecting - the catch path swallows the autoFix failure and lets startup proceed.
    await assert.doesNotReject(() => runConsistencyProbeAtStartup(),
      "the probe must complete cleanly even when an autoFix throws - the catch is the safety net for partial-progress startup");

    // The catch path's diagnostic surfaces the failed category for operator triage. The exact log shape is "Consistency probe auto-fix failed for %s: %s." with
    // the category as the first argument.
    const swallowed = lines().filter((line: CapturedLogLine) => (line.level === "warn") && line.message.includes("Consistency probe auto-fix failed"));

    assert.ok(swallowed.length >= 1, "the swallowed autoFix failure is logged at warn level");
    assert.equal(swallowed[0]?.args[0], "unknown-service-tag",
      "the failing category surfaces as a structured arg so operators can identify which auto-fix broke");
  });
});
