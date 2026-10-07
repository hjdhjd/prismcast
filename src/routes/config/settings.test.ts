/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * settings.test.ts: Unit tests for the settings UI generators and the route-aggregator wiring in settings.ts. The HTML generators (Settings tab,
 * Advanced tab, collapsible section, footer) are pure functions of the loaded snapshot (the saved configuration), CONFIG_METADATA, getSettingsTabSections,
 * getAdvancedSections, and the environment-override map their caller passes in - the page render resolves that map once and every generator draws the
 * disabled fields and badges from the copy it was handed. Internal helpers (formatValueForDisplay, parseFormValue, validateSettingValue, etc.) are not
 * exported and are exercised through the public surface. The route-aggregator setupSettingsRoutes has its pre-I/O validation short-circuit branches
 * exercised directly through the Express stub's invoke helper. The rows that save or import go through the real file store in a data directory of their own;
 * the restart-scheduling continuation stays untested here because it requires a live Express runtime.
 */
import { CONFIG, getLoadedConfiguration, initializeConfiguration } from "../../config/index.ts";
import { afterEach, beforeEach, describe, test } from "node:test";
import { closePuppeteerStreamWssOnIdle, withTempDir } from "../../testing.helpers.ts";
import { generateAdvancedTabContent, generateCollapsibleSection, generateSettingsFormFooter, generateSettingsTabContent,
  setupSettingsRoutes } from "./settings.ts";
import { getAdvancedSections, getSettingByPath, readConfig } from "../../config/userConfig.ts";
import { getConfigFilePath, initializeDataDir } from "../../config/paths.ts";
import { registerConfigChangeHandler, resetConfigChangeHandlers } from "../../config/reactivity.ts";
import type { AdvancedSection } from "../../config/userConfig.ts";
import type { ChangeRejection } from "../../config/reactivity.ts";
import type { ConfigStore } from "../../config/index.ts";
import { VIDEO_QUALITY_PRESETS } from "../../config/presets.ts";
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { makeExpressStub } from "../express.helpers.ts";
import os from "node:os";

/* The Settings footer reads the resolved data directory via getDataDir(). In production this is set during startup; in tests we initialize it once
 * with a temp-dir-style path so the footer renderer (and any path resolution downstream) can produce a deterministic value. We use os.tmpdir() as a
 * stable absolute path that exists on all platforms; we never write to it.
 */
initializeDataDir(os.tmpdir());

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

/* The Express stub helper (makeExpressStub) is shared across route tests; it lives in ../express.helpers.ts.
 */

describe("generateSettingsTabContent", () => {

  test("returns a non-empty HTML string for a fresh process with default config", () => {

    const html = generateSettingsTabContent(new Map());

    assert.ok(html.length > 0, "Settings tab content renders");
    assert.ok(typeof html === "string", "returns a string");
  });

  test("offers every quality preset by friendly name, with no display-driven qualifier or warning", () => {

    /* The preset dropdown is rendered from the preset table itself, so the list of options is the list of presets and each label is the preset's own name. Every
     * preset is offered unconditionally: capture renders at whichever one is chosen, because the surface is emulated rather than taken from the display. The
     * negative halves are the point - a qualifier on a label, or a warning under the field, would be telling the operator their choice will not be honoured.
     */
    const html = generateSettingsTabContent(new Map()) + generateAdvancedTabContent(new Map());

    for(const preset of VIDEO_QUALITY_PRESETS) {

      assert.ok(html.includes(">" + preset.name + "</option>"), preset.id + " is offered under its own name");
      assert.ok(html.includes("value=\"" + preset.id + "\""), preset.id + " is offered under its own id");
    }

    assert.equal(html.includes("limited to"), false, "no option label carries a display-driven qualifier");
    assert.equal(html.includes("Your display cannot support this resolution"), false, "no display warning is rendered under the field");
  });

  test("renders the panel header with the reset-to-defaults link", () => {

    const html = generateSettingsTabContent(new Map());

    assert.match(html, /class="panel-header"/, "panel-header div is present");
    assert.match(html, /class="panel-reset"/, "reset link is present");
    assert.match(html, /data-click-action="reset-tab-to-defaults" data-tab="settings"/, "reset link targets the settings tab");
  });

  test("renders one settings-section block per section", () => {

    // The Settings tab is composed of ordered sections (Server, Browser, Startup, Capture, etc.) each emitted as a settings-section div with a
    // header. We count the section divs to ensure none are dropped silently. The exact count depends on SETTINGS_TAB_SECTIONS but must always be
    // at least 1.
    const html = generateSettingsTabContent(new Map());
    const sectionMatches = html.match(/class="settings-section"/g) ?? [];

    assert.ok(sectionMatches.length >= 1, "at least one settings-section is rendered");
  });

  test("includes section header divs for each settings-section", () => {

    const html = generateSettingsTabContent(new Map());
    const headerMatches = html.match(/class="settings-section-header"/g) ?? [];

    assert.ok(headerMatches.length >= 1, "section-header markup is present");
  });

  test("renders form-group divs for individual settings (no validation errors path)", () => {

    // Each setting in the Settings tab emits a form-group wrapper. With at least one section containing settings, at least one form-group must be
    // present.
    const html = generateSettingsTabContent(new Map());

    assert.match(html, /class="form-group/, "at least one form-group is rendered");
  });

  test("returns a deterministic string across two calls with no overrides (repeat-safe renderer)", () => {

    // Locks renderer determinism: callers comparing rendered HTML across edits should see diffs only from data changes, not renderer flakiness.
    const a = generateSettingsTabContent(new Map());
    const b = generateSettingsTabContent(new Map());

    assert.equal(a, b, "two calls produce identical HTML");
  });
});

describe("generateAdvancedTabContent", () => {

  test("returns a non-empty HTML string", () => {

    const html = generateAdvancedTabContent(new Map());

    assert.ok(html.length > 0, "Advanced tab renders");
  });

  test("renders the panel header with the reset-all-to-defaults link", () => {

    const html = generateAdvancedTabContent(new Map());

    assert.match(html, /class="panel-header"/, "panel-header div is present");
    assert.match(html, /data-click-action="reset-tab-to-defaults" data-tab="advanced"/, "reset link targets the advanced tab");
    assert.match(html, /Reset All to Defaults/, "Advanced uses the all-encompassing reset wording");
  });

  test("emits one advanced-section per Advanced section", () => {

    // The Advanced tab uses collapsible sections (advanced-section class). Verify at least one is rendered.
    const html = generateAdvancedTabContent(new Map());

    assert.match(html, /class="advanced-section"/, "advanced-section divs are present");
  });

  test("each advanced-section has a clickable header to toggle collapse", () => {

    const html = generateAdvancedTabContent(new Map());

    assert.match(html, /data-click-action="toggle-section"/, "section headers wire up the toggle-section action");
    assert.match(html, /class="section-chevron"/, "chevron indicator is rendered");
  });

  test("renders the section count suffix in the header (singular for 1, plural otherwise)", () => {

    // The renderer emits "(N setting)" for 1 and "(N settings)" for N != 1. Verify that one of these patterns appears.
    const html = generateAdvancedTabContent(new Map());

    // Either " setting)" (with no trailing 's') or " settings)" must appear at least once.
    const hasSingular = html.includes(" setting)");
    const hasPlural = html.includes(" settings)");

    assert.ok(hasSingular || hasPlural, "section header includes the count suffix");
  });

  test("returns a deterministic string across two calls", () => {

    const a = generateAdvancedTabContent(new Map());
    const b = generateAdvancedTabContent(new Map());

    assert.equal(a, b, "two calls produce identical HTML");
  });
});

describe("generateCollapsibleSection", () => {

  test("returns an empty-section shell when given a section with no settings", () => {

    // Boundary: an empty settings array should still render the section frame. The count text must read "(0 settings)" because 0 is plural in this
    // implementation (only the value 1 takes the singular form).
    const empty: AdvancedSection = { displayName: "Empty Section", id: "empty", settings: [] };
    const html = generateCollapsibleSection(empty, new Map());

    assert.match(html, /class="advanced-section"/, "section wrapper rendered");
    assert.match(html, /Empty Section/, "displayName rendered");
    assert.match(html, /\(0 settings\)/, "count uses plural form for 0");
  });

  test("escapes HTML in the section displayName", () => {

    // Guard against XSS in any future user-provided section name. The renderer pipes displayName through escapeHtml.
    const evil: AdvancedSection = { displayName: "<script>alert(1)</script>", id: "evil", settings: [] };
    const html = generateCollapsibleSection(evil, new Map());

    assert.match(html, /&lt;script&gt;/, "script tag is HTML-escaped");
    assert.doesNotMatch(html, /<script>alert/, "raw script tag must not appear");
  });

  test("escapes HTML in the section id used as the data attribute", () => {

    // Boundary: the id is interpolated into both the outer data-section= wrapper attribute and the inner data-section-id= header attribute - both must escape.
    const tricky: AdvancedSection = { displayName: "Tricky", id: "id\"with-quote", settings: [] };
    const html = generateCollapsibleSection(tricky, new Map());

    assert.match(html, /data-section="id&quot;with-quote"/, "id is escaped in data-section attribute");
  });

  test("renders the section-header data-click-action wired to toggle-section with the section id", () => {

    const section: AdvancedSection = { displayName: "Foo", id: "foo", settings: [] };
    const html = generateCollapsibleSection(section, new Map());

    assert.match(html, /data-click-action="toggle-section" data-section-id="foo"/, "toggle-section action is wired");
  });

  test("uses singular 'setting' for exactly 1 setting (boundary)", () => {

    // The first real Advanced section we have access to provides a SettingMetadata sample. We construct a synthetic section with one of its settings.
    const sections = getAdvancedSections();
    const firstWithSetting = sections.find((s) => s.settings.length > 0);

    if(firstWithSetting === undefined) {

      // Defensive: the codebase always has at least one Advanced setting, but we guard against an unusual config.
      return;
    }

    const synthetic: AdvancedSection = {

      displayName: "OneSetting",
      id: "one-setting",
      settings: [firstWithSetting.settings[0]!]
    };

    const html = generateCollapsibleSection(synthetic, new Map());

    assert.match(html, /\(1 setting\)/, "exactly 1 uses singular form");
    assert.doesNotMatch(html, /\(1 settings\)/, "must not use plural for 1");
  });
});

describe("the environment-override map the render passes down", () => {

  /* The generators take the override map as a parameter rather than reading process.env themselves, so one page render resolves it once and every section
   * draws from the same copy. These rows separate the two sources: the map decides what renders, and the environment on its own decides nothing. A generator
   * that went back to getEnvOverrides() would pass the first row by luck only when the environment happened to agree, and would fail the second outright.
   */
  const OVERRIDDEN_ADVANCED_PATH = "hls.maxSegments";
  const OVERRIDDEN_ADVANCED_VAR = "HLS_MAX_SEGMENTS";
  const OVERRIDDEN_SETTINGS_PATH = "server.port";
  const OVERRIDDEN_SETTINGS_VAR = "PORT";

  let originalAdvanced: string | undefined;
  let originalPort: string | undefined;

  const hlsSection = (): AdvancedSection => {

    const section = getAdvancedSections().find((candidate) => candidate.settings.some((setting) => setting.path === OVERRIDDEN_ADVANCED_PATH));

    assert.ok(section, "the advanced section carrying " + OVERRIDDEN_ADVANCED_PATH + " is present");

    return section;
  };

  beforeEach(() => {

    originalAdvanced = process.env[OVERRIDDEN_ADVANCED_VAR];
    originalPort = process.env[OVERRIDDEN_SETTINGS_VAR];
    Reflect.deleteProperty(process.env, OVERRIDDEN_ADVANCED_VAR);
    Reflect.deleteProperty(process.env, OVERRIDDEN_SETTINGS_VAR);
  });

  afterEach(() => {

    for(const [ name, value ] of [ [ OVERRIDDEN_ADVANCED_VAR, originalAdvanced ], [ OVERRIDDEN_SETTINGS_VAR, originalPort ] ] as const) {

      if(value === undefined) {

        Reflect.deleteProperty(process.env, name);

        continue;
      }

      process.env[name] = value;
    }
  });

  test("disables the fields the passed map names and renders their badges, with the environment holding neither variable", () => {

    const settingsHtml = generateSettingsTabContent(new Map([[ OVERRIDDEN_SETTINGS_PATH, "8080" ]]));
    const sectionHtml = generateCollapsibleSection(hlsSection(), new Map([[ OVERRIDDEN_ADVANCED_PATH, "42" ]]));

    assert.match(settingsHtml, /id="server-port"[^>]*disabled/, "the settings-tab field the map names renders disabled");
    assert.match(settingsHtml, /<code>PORT=8080<\/code>/, "the settings-tab field carries the badge the map's value produced");
    assert.match(sectionHtml, /id="hls-maxSegments"[^>]*disabled/, "the collapsible-section field the map names renders disabled");
    assert.match(sectionHtml, /<code>HLS_MAX_SEGMENTS=42<\/code>/, "the collapsible-section field carries the badge the map's value produced");
  });

  test("leaves the fields an empty map omits editable and unbadged, with the environment holding both variables", () => {

    process.env[OVERRIDDEN_SETTINGS_VAR] = "9090";
    process.env[OVERRIDDEN_ADVANCED_VAR] = "77";

    const settingsHtml = generateSettingsTabContent(new Map());
    const sectionHtml = generateCollapsibleSection(hlsSection(), new Map());

    assert.doesNotMatch(settingsHtml, /id="server-port"[^>]*disabled/, "the settings tab renders from the map it was handed, not the environment");
    assert.doesNotMatch(settingsHtml, /PORT=9090/, "no badge is drawn for an override the passed map does not carry");
    assert.doesNotMatch(sectionHtml, /id="hls-maxSegments"[^>]*disabled/, "the collapsible section renders from the map it was handed, not the environment");
    assert.doesNotMatch(sectionHtml, /HLS_MAX_SEGMENTS=77/, "no badge is drawn for an override the passed map does not carry");
  });
});

// A store that reads an empty file, which re-initializes CONFIG and the loaded snapshot to the defaults. Only the boot reads through it.
const emptyStore: ConfigStore = {

  mutateConfig: async (): Promise<void> => {

    throw new Error("The empty store takes no writes.");
  },
  readConfig: async () => ({ config: {}, parseError: false, readError: false })
};

/* The form renders the saved configuration, the loaded snapshot, rather than the running one. Each row seeds the two apart through the module's own save: the
 * form posts hdhr.enabled false and a test handler refuses it, so the file and the loaded snapshot turn HDHomeRun off while CONFIG keeps it on. Only src/app.ts
 * imports the HDHomeRun module, so the row registers the refusing handler for the full path itself. The save writes through the real file store, so each row
 * takes a data directory of its own rather than the os.tmpdir() this file initializes, and each row re-initializes CONFIG and the snapshot to the defaults.
 */
describe("the form renders the saved configuration", () => {

  /**
   * Reads whether the HDHomeRun enable checkbox in a rendered fragment is checked.
   * @param html - The rendered fragment.
   * @returns True when the checkbox carries the checked attribute.
   */
  function isEnabledChecked(html: string): boolean {

    const tag = (/<input class="form-checkbox" type="checkbox" id="hdhr-enabled"[^>]*>/).exec(html)?.[0];

    assert.ok(tag !== undefined, "the HDHomeRun enable checkbox renders");

    return / checked(?=[ >])/.test(tag);
  }

  /**
   * Reads the class list of the form group that renders a field, from the group's opening tag.
   * @param html - The rendered fragment.
   * @param inputId - The id of the field's input.
   * @returns The group's classes.
   */
  function groupClassesOf(html: string, inputId: string): string[] {

    const group = html.split("<div class=\"form-group").find((chunk) => chunk.includes("for=\"" + inputId + "\""));

    assert.ok(group !== undefined, "the " + inputId + " group renders");

    return ("form-group" + group.slice(0, group.indexOf("\""))).split(" ");
  }

  /**
   * Saves hdhr.enabled false through the settings save with a handler refusing it, so the saved configuration turns HDHomeRun off while CONFIG keeps it on.
   * @param dir - The row's data directory.
   */
  async function saveRefusedDisable(dir: string): Promise<void> {

    initializeDataDir(dir);
    registerConfigChangeHandler("hdhr.enabled", async (changes): Promise<readonly ChangeRejection[]> => changes.map((change) => ({

      path: change.path,
      reason: "The HDHomeRun surface refused the change."
    })));

    const { app, invoke } = makeExpressStub();

    setupSettingsRoutes(app as never);

    const result = await invoke("post", "/config", { body: { hdhr: { enabled: false } } });

    assert.equal(result.statusCode, 200, "precondition: the save completed");
    assert.equal(CONFIG.hdhr.enabled, true, "precondition: the refusal kept HDHomeRun on in the running configuration");
    assert.equal(getLoadedConfiguration().hdhr.enabled, false, "precondition: the saved configuration turns HDHomeRun off");
  }

  beforeEach(async () => {

    resetConfigChangeHandlers();
    await initializeConfiguration(undefined, emptyStore);
  });

  afterEach(async () => {

    resetConfigChangeHandlers();
    await initializeConfiguration(undefined, emptyStore);
    initializeDataDir(os.tmpdir());
  });

  test("the Settings tab renders a field from the saved configuration, not the running one", async () => {

    await withTempDir(async (dir) => {

      assert.equal(isEnabledChecked(generateSettingsTabContent(new Map())), true, "precondition: the field renders checked while the saved and running values agree");

      await saveRefusedDisable(dir);

      assert.equal(isEnabledChecked(generateSettingsTabContent(new Map())), false, "the field shows the saved value");
    });
  });

  test("a collapsible section renders a field from the saved configuration, not the running one", async () => {

    const setting = getSettingByPath("hdhr.enabled");

    assert.ok(setting !== undefined, "precondition: the setting exists");

    const section: AdvancedSection = { displayName: "HDHomeRun", id: "hdhr", settings: [setting] };

    await withTempDir(async (dir) => {

      assert.equal(isEnabledChecked(generateCollapsibleSection(section, new Map())), true,
        "precondition: the field renders checked while the saved and running values agree");

      await saveRefusedDisable(dir);

      assert.equal(isEnabledChecked(generateCollapsibleSection(section, new Map())), false, "the field shows the saved value");
    });
  });

  test("the dependsOn grey-out follows the toggle in the saved configuration, not the running one", async () => {

    await withTempDir(async (dir) => {

      const before = groupClassesOf(generateSettingsTabContent(new Map()), "hdhr-port");

      assert.ok(before.includes("form-group"), "precondition: the port's group renders");
      assert.equal(before.includes("depends-disabled"), false, "precondition: the port is not greyed while the saved and running values agree");

      await saveRefusedDisable(dir);

      assert.ok(groupClassesOf(generateSettingsTabContent(new Map()), "hdhr-port").includes("depends-disabled"), "the port greys out from the saved toggle");
    });
  });
});

/* An import document is merged into the file inside the save, so a key that names prototype machinery would reach a prototype rather than a setting. Each
 * document is built from JSON text, because JSON.parse keeps such a key as an ordinary own property where an object literal would set the prototype instead.
 * The import goes through the real file store, so each row takes a data directory of its own.
 */
describe("POST /config/import - a key that is not a setting", () => {

  afterEach(() => {

    // A key that got through would leave its write on a shared object for every later row in this process, so each row clears every place it could land.
    Reflect.deleteProperty(Object.prototype, "polluted");
    Reflect.deleteProperty(Object, "polluted");
    initializeDataDir(os.tmpdir());
  });

  for(const key of [ "__proto__", "constructor", "prototype" ]) {

    for(const [ level, document ] of [ [ "the top level", "{\"" + key + "\": {\"polluted\": true}}" ],
      [ "the category level", "{\"hdhr\": {\"" + key + "\": {\"polluted\": true}}}" ] ] as const) {

      test("answers a validation error for a " + key + " key at " + level + ", writes nothing, and leaves the prototype untouched", async () => {

        await withTempDir(async (dir) => {

          initializeDataDir(dir);

          const { app, invoke } = makeExpressStub();

          setupSettingsRoutes(app as never);

          const result = await invoke("post", "/config/import", { body: JSON.parse(document) as unknown });

          assert.equal(result.statusCode, 400, "the import answers a validation error");
          assert.equal((result.body as { error: string }).error, "The configuration document carries the key " + key + ", which is not a setting.",
            "the import is refused by the key it carries");
          assert.equal(Object.hasOwn(Object.prototype, "polluted"), false, "the shared prototype is untouched");
          assert.equal(existsSync(getConfigFilePath()), false, "nothing was written");
        });
      });
    }
  }
});

/* The validators hold a numeric setting to the minimum its metadata declares, and recovery.backoffJitter declares zero, so zero is a value the settings form and
 * the import accept. Each row posts zero through the real file store in a data directory of its own and reads the value back from the file and the running
 * configuration.
 */
describe("a setting whose declared minimum is zero accepts zero", () => {

  beforeEach(async () => {

    await initializeConfiguration(undefined, emptyStore);
  });

  afterEach(async () => {

    await initializeConfiguration(undefined, emptyStore);
    initializeDataDir(os.tmpdir());
  });

  test("the settings form saves zero for recovery.backoffJitter, and the value reaches the file and the running configuration", async () => {

    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      assert.equal(getSettingByPath("recovery.backoffJitter")?.min, 0, "precondition: the metadata declares a floor of zero");
      assert.equal(CONFIG.recovery.backoffJitter, 1000, "precondition: the running value is the default");

      const { app, invoke } = makeExpressStub();

      setupSettingsRoutes(app as never);

      const result = await invoke("post", "/config", { body: { recovery: { backoffJitter: 0 } } });

      assert.equal((result.body as { errors?: Record<string, string> }).errors, undefined, "the form raises no validation error for zero");
      assert.equal(result.statusCode, 200, "the save completes");
      assert.equal((await readConfig()).config.recovery?.backoffJitter, 0, "the file holds zero");
      assert.equal(CONFIG.recovery.backoffJitter, 0, "the live setting is committed to the running configuration");
    });
  });

  test("an import of zero for recovery.backoffJitter is accepted, and the value reaches the file and the running configuration", async () => {

    await withTempDir(async (dir) => {

      initializeDataDir(dir);

      assert.equal(CONFIG.recovery.backoffJitter, 1000, "precondition: the running value is the default");

      const { app, invoke } = makeExpressStub();

      setupSettingsRoutes(app as never);

      const result = await invoke("post", "/config/import", { body: { recovery: { backoffJitter: 0 } } });

      assert.equal((result.body as { error?: string }).error, undefined, "the import raises no validation error for zero");
      assert.equal(result.statusCode, 200, "the import completes");
      assert.equal((await readConfig()).config.recovery?.backoffJitter, 0, "the file holds zero");
      assert.equal(CONFIG.recovery.backoffJitter, 0, "the live setting is committed to the running configuration");
    });
  });
});

describe("generateSettingsFormFooter", () => {

  test("returns a div containing the literal 'Configuration file' label", () => {

    const html = generateSettingsFormFooter();

    assert.match(html, /class="config-path"/, "wrapper class is present");
    assert.match(html, /Configuration file:/, "label text is present");
  });

  test("includes a <code> element holding the resolved config file path", () => {

    const html = generateSettingsFormFooter();

    assert.match(html, /<code>.+<\/code>/, "config path is wrapped in <code>");
  });

  test("returns a non-empty string", () => {

    const html = generateSettingsFormFooter();

    assert.ok(html.length > 0, "footer renders");
  });

  test("returns a deterministic value across two calls", () => {

    const a = generateSettingsFormFooter();
    const b = generateSettingsFormFooter();

    assert.equal(a, b, "footer is deterministic in a single process (config path is fixed)");
  });
});

/* The validation paths in setupSettingsRoutes return 4xx envelopes before touching disk-backed state (applyConfigurationChange, scheduleServerRestart). They are pure
 * functions of req shape modulo CONFIG_METADATA, which is a static module export. We exercise them via the stub's invoke helper, which constructs a minimal req and
 * captures the JSON envelope written to res. The /config/restart-now handler is also covered here because its non-service guard returns 400 without touching any I/O.
 */
describe("setupSettingsRoutes - validation handlers (invoked via Express stub)", () => {

  test("POST /config - returns 400 with errors map when a setting fails validation", async () => {

    const { app, invoke } = makeExpressStub();

    setupSettingsRoutes(app as never);

    /* browser.initTimeout has min=100, max=30000 (ms storage; displayDivisor=1000 means seconds in display). Sending an extreme value lands the parsed value
     * outside the validation window and produces an entry in the validationErrors map.
     */
    const result = await invoke("post", "/config", { body: { browser: { initTimeout: 999999 } } });

    assert.equal(result.statusCode, 400);
    assert.equal((result.body as { success: boolean }).success, false);

    const errors = (result.body as { errors: Record<string, string> }).errors;

    assert.ok(typeof errors === "object", "errors is an object");
    assert.ok("browser.initTimeout" in errors, "the offending path is keyed in the errors map");
  });

  test("POST /config/import - returns 400 when body is not an object", async () => {

    const { app, invoke } = makeExpressStub();

    setupSettingsRoutes(app as never);

    const result = await invoke("post", "/config/import", { body: "not an object" });

    assert.equal(result.statusCode, 400);
    assert.match((result.body as { error: string }).error, /Invalid configuration format/);
  });

  test("POST /config/import - returns 400 when body is null", async () => {

    const { app, invoke } = makeExpressStub();

    setupSettingsRoutes(app as never);

    const result = await invoke("post", "/config/import", { body: null });

    assert.equal(result.statusCode, 400);
    assert.match((result.body as { error: string }).error, /Invalid configuration format/);
  });

  test("POST /config/import - returns 400 when body is an array (object-but-not-record)", async () => {

    const { app, invoke } = makeExpressStub();

    setupSettingsRoutes(app as never);

    const result = await invoke("post", "/config/import", { body: [ 1, 2, 3 ] });

    assert.equal(result.statusCode, 400);
    assert.match((result.body as { error: string }).error, /Invalid configuration format/);
  });

  test("POST /config/import - returns 400 when a known category is the wrong shape", async () => {

    // browser is a real CONFIG_METADATA category. Passing a string for it (rather than an object) trips the "expected an object" branch.
    const { app, invoke } = makeExpressStub();

    setupSettingsRoutes(app as never);

    const result = await invoke("post", "/config/import", { body: { browser: "not an object" } });

    assert.equal(result.statusCode, 400);
    assert.match((result.body as { error: string }).error, /Invalid browser configuration: expected an object/);
  });

  test("POST /config/import - returns 400 when a setting value fails its validation rule", async () => {

    // browser.initTimeout has a numeric min/max. A value below min is rejected and surfaces in the validationErrors list with the setting's label.
    const { app, invoke } = makeExpressStub();

    setupSettingsRoutes(app as never);

    const result = await invoke("post", "/config/import", { body: { browser: { initTimeout: 1 } } });

    assert.equal(result.statusCode, 400);
    assert.match((result.body as { error: string }).error, /Validation errors/);
  });

  test("POST /config/restart-now - returns 400 with 'not running as a service' guard when not a managed service", async () => {

    // In the unit-test process there is no service manager environment variable set, so isRunningAsService() returns false. The handler returns 400 with the
    // canonical message; this is the only branch in the entire handler that doesn't trigger a real restart, so it's also the only branch that's safe to test.
    const { app, invoke } = makeExpressStub();

    setupSettingsRoutes(app as never);

    const result = await invoke("post", "/config/restart-now", {});

    assert.equal(result.statusCode, 400);
    assert.equal((result.body as { success: boolean }).success, false);
    assert.match((result.body as { error: string }).error, /not running as a service/);
  });
});

describe("setupSettingsRoutes", () => {

  test("registers the documented settings endpoints", () => {

    /* The aggregator wires:
     *   POST /config
     *   GET  /config/export
     *   POST /config/import
     *   POST /config/restart-now
     */
    const { app, calls } = makeExpressStub();

    setupSettingsRoutes(app as never);

    const has = (method: string, path: string): boolean => calls.some((c) => (c.method === method) && (c.path === path));

    assert.ok(has("post", "/config"), "POST /config is registered");
    assert.ok(has("get", "/config/export"), "GET /config/export is registered");
    assert.ok(has("post", "/config/import"), "POST /config/import is registered");
    assert.ok(has("post", "/config/restart-now"), "POST /config/restart-now is registered");
  });

  test("registers exactly the documented number of routes (no extras, no drops)", () => {

    // Assert the count so a route added or removed forces an explicit test update.
    const { app, calls } = makeExpressStub();

    setupSettingsRoutes(app as never);

    assert.equal(calls.length, 4, "expected exactly 4 routes registered");
  });

  test("does not throw on a stub Express app", () => {

    const { app } = makeExpressStub();

    assert.doesNotThrow(() => {

      setupSettingsRoutes(app as never);
    }, "registration should be side-effect-free at the app-stub level");
  });
});
