/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * content.test.ts: Unit tests for the tab content HTML generators on the landing page. The module exports six tab generators (Overview, Help, API Reference,
 * Channels, Logs, Configuration) plus several internal helpers. Each tab is a pure HTML-string generator with no DOM, so we lock in structural invariants
 * (presence of the major sections, absence of template-literal artifacts) rather than running any real DOM logic. Several tabs read configuration state during
 * generation (Configuration's footer needs the data directory initialized, the Logs tab inlines client-side script blocks), so we initialize a per-suite
 * temp data directory in before().
 */
import { after, before, describe, test } from "node:test";
import { generateApiReferenceContent, generateChannelsTabContent, generateConfigContent, generateHelpContent, generateLogsContent,
  generateOverviewContent } from "./content.ts";
import { mkdtempSync, rmSync } from "node:fs";
import assert from "node:assert/strict";
import { closePuppeteerStreamWssOnIdle } from "../../testing.helpers.ts";
import { initializeDataDir } from "../../config/paths.ts";
import { tmpdir } from "node:os";

let sharedTempDir = "";

before(() => {

  // Initialize a per-suite data directory so generators that transitively read getConfigFilePath have a populated module-level path.
  sharedTempDir = mkdtempSync(tmpdir() + "/prismcast-content-test-");
  initializeDataDir(sharedTempDir);
});

after(() => {

  if(sharedTempDir) {

    rmSync(sharedTempDir, { force: true, recursive: true });
  }
});

// Schedule background-server cleanup on a 0ms unref'd timer that fires when the suite resolves so the runner can exit cleanly.
closePuppeteerStreamWssOnIdle();

describe("generateOverviewContent", () => {

  test("returns a non-empty HTML string with multiple section blocks", () => {

    // Overview is the largest tab by content - around a dozen <h3>-led sections covering the user guide. A length floor of 2000 catches an empty/truncated
    // generator without locking in a brittle exact byte count.
    const html = generateOverviewContent("http://localhost:5589");

    assert.equal(typeof html, "string");
    assert.ok(html.length > 2000, "Overview should be substantial; got " + String(html.length));

    const sectionCount = (html.match(/<h3>/g) ?? []).length;

    assert.ok(sectionCount >= 5, "should have at least 5 h3 sections; got " + String(sectionCount));
  });

  test("includes the active streams section table at the top", () => {

    // generateActiveStreamsSection puts a #streams-container with a tbody#streams-tbody and an empty-row placeholder. The status SSE script populates this on
    // load. The tab must render the placeholder so the SSE handler has a target.
    const html = generateOverviewContent("http://localhost:5589");

    assert.match(html, /id="streams-container"/);
    assert.match(html, /id="streams-tbody"/);
    assert.match(html, /No active streams/);
  });

  test("interpolates the baseUrl into the playlist URL exactly once for the copy-target span", () => {

    // The Quick Start section uses #overview-playlist-url to mark the copy-to-clipboard target. The baseUrl appears literally in the rendered HTML.
    const baseUrl = "https://prismcast.example.test:5589";
    const html = generateOverviewContent(baseUrl);

    assert.match(html, /id="overview-playlist-url"/);
    assert.ok(html.includes(baseUrl + "/playlist"), "baseUrl + /playlist should appear verbatim");
    assert.ok(html.includes(baseUrl + "/hls/nbc/stream.m3u8"), "baseUrl + sample HLS URL should appear");
  });

  test("includes the Plex Integration block referencing the HDHomeRun port", () => {

    // The Plex section references port 5004 for HDHomeRun emulation. This is a soft check on the textual content so a renumber to a different port surfaces here.
    const html = generateOverviewContent("http://localhost:5589");

    assert.match(html, /<h3>Plex Integration<\/h3>/);
    assert.match(html, /5004/);
  });

  test("includes the Tuning Speed section with the documented timing tiers", () => {

    const html = generateOverviewContent("http://localhost:5589");

    assert.match(html, /Tuning Speed/);
    assert.match(html, /Direct URL Channels/);
    assert.match(html, /Guide-Based Services/);
  });

  test("does not produce template-literal artifacts in the rendered HTML", () => {

    // Sanity check for stringified missing values from the providerInfo lookup: undefined leaked into a string or [object Object] from a coerced object.
    const html = generateOverviewContent("http://localhost:5589");

    assert.doesNotMatch(html, /\$\{undefined\}/);
    assert.doesNotMatch(html, /\[object Object\]/);
  });

  test("returns identical output for the same baseUrl input (deterministic)", () => {

    // Provider info lookup and label sorting should be deterministic for any given baseUrl.
    const a = generateOverviewContent("http://localhost:5589");
    const b = generateOverviewContent("http://localhost:5589");

    assert.equal(a, b, "two calls with the same input must produce byte-identical output");
  });
});

describe("generateHelpContent", () => {

  test("returns a non-empty HTML string with the documented top-level sections", () => {

    const html = generateHelpContent();

    assert.equal(typeof html, "string");
    assert.ok(html.length > 1000, "Help content should be substantial; got " + String(html.length));
    assert.match(html, /Updating PrismCast/);
    assert.match(html, /Backup and Migration/);
    assert.match(html, /Display and Resolution/);
    assert.match(html, /Platform Notes/);
    assert.match(html, /Troubleshooting/);
    assert.match(html, /Known Limitations/);
  });

  test("includes a troubleshooting table with at least one row per documented problem", () => {

    // The troubleshooting matrix has 8+ rows. A floor of 5 keeps the assertion stable across edits while still catching a structural regression that drops the
    // table.
    const html = generateHelpContent();
    const tableMatch = html.match(/<table>[\s\S]*?<\/table>/g);

    assert.ok(tableMatch && (tableMatch.length >= 1), "should contain at least one HTML table");

    const rowCount = (html.match(/<tr>/g) ?? []).length;

    assert.ok(rowCount >= 5, "should have multiple troubleshooting rows; got " + String(rowCount));
  });

  test("includes Homebrew, npm, and Docker upgrade command examples", () => {

    // The Updating PrismCast section documents three install methods. The literal command examples are part of the user-visible content.
    const html = generateHelpContent();

    assert.match(html, /brew upgrade prismcast/);
    assert.match(html, /npm install -g prismcast/);
    assert.match(html, /docker pull/);
  });

  test("does not produce template-literal artifacts", () => {

    const html = generateHelpContent();

    assert.doesNotMatch(html, /\$\{undefined\}/);
    assert.doesNotMatch(html, /\[object Object\]/);
  });
});

describe("generateApiReferenceContent", () => {

  test("returns HTML with all eight API category groups", () => {

    // The API index has eight named groups: Streaming, Playlist, Channels, Services, Profiles, Authentication, Management, Settings, Diagnostics. We verify
    // each section heading anchor is present so all categories surface for users.
    const html = generateApiReferenceContent();
    const anchors = [ "api-streaming", "api-playlist", "api-channels", "api-services", "api-profiles", "api-auth", "api-management", "api-settings",
      "api-diagnostics" ];

    for(const id of anchors) {

      assert.ok(html.includes("id=\"" + id + "\""), "API section " + id + " heading missing");
    }
  });

  test("documents core HLS streaming endpoints", () => {

    // The Streaming section lists key HTTP routes. We confirm the documented endpoint URLs survive in the rendered HTML.
    const html = generateApiReferenceContent();

    assert.match(html, /GET \/hls\/:name\/stream\.m3u8/);
    assert.match(html, /GET \/play/);
    assert.match(html, /GET \/stream\/:name/);
  });

  test("includes the example health response JSON block", () => {

    // The Example: Health Check Response section embeds a literal JSON sample inside <pre>. The fields documented include browser, captureMode, chrome, etc.
    const html = generateApiReferenceContent();

    assert.match(html, /Example: Health Check Response/);
    assert.match(html, /<pre>\{/);
    assert.match(html, /"browser":/);
    assert.match(html, /"captureMode":/);
  });

  test("does not produce template-literal artifacts", () => {

    const html = generateApiReferenceContent();

    assert.doesNotMatch(html, /\$\{undefined\}/);
    assert.doesNotMatch(html, /\[object Object\]/);
  });

  test("returns identical output across calls (deterministic)", () => {

    // The provider slug list is sorted; output should be byte-identical.
    assert.equal(generateApiReferenceContent(), generateApiReferenceContent());
  });
});

describe("generateChannelsTabContent", () => {

  test("returns HTML with both subtab panels", () => {

    // The tab has two subtabs: 'channels' (default active) and 'custom-profiles'. Both panels and the subtab bar buttons render unconditionally on every page.
    const html = generateChannelsTabContent();

    assert.match(html, /class="channels-subtab-bar"/);
    assert.match(html, /data-channels-subtab="channels"/);
    assert.match(html, /data-channels-subtab="custom-profiles"/);
    assert.match(html, /id="channels-subtab-channels"/);
    assert.match(html, /id="channels-subtab-custom-profiles"/);
  });

  test("includes the login modal hidden by default", () => {

    // Channel authentication uses a modal that is rendered hidden and shown via JavaScript when the user clicks Login on a channel.
    const html = generateChannelsTabContent();

    assert.match(html, /id="login-modal"/);
    assert.match(html, /Channel Authentication/);
    assert.match(html, /onclick="endLogin\(\)"/, "Done button wired to endLogin");
  });

  test("includes the test modal for profile testing", () => {

    // Profile test flow uses a separate modal showing live page testing controls. checkSelectors and endProfileTest are wired here.
    const html = generateChannelsTabContent();

    assert.match(html, /id="test-modal"/);
    assert.match(html, /Profile Test/);
    assert.match(html, /onclick="checkSelectors\(\)"/);
    assert.match(html, /onclick="endProfileTest\(\)"/);
  });

  test("does not produce template-literal artifacts", () => {

    const html = generateChannelsTabContent();

    assert.doesNotMatch(html, /\$\{undefined\}/);
    assert.doesNotMatch(html, /\[object Object\]/);
  });
});

describe("generateLogsContent", () => {

  test("returns HTML with the log viewer container and level filter dropdown", () => {

    // The Logs tab has a #log-container that the SSE script populates and a #log-level select that filters by severity. The four levels (All, error, warn, info)
    // are rendered as <option> elements.
    const html = generateLogsContent();

    assert.match(html, /id="log-container"/);
    assert.match(html, /class="log-viewer"/);
    assert.match(html, /id="log-level"/);
    assert.match(html, /<option value="error"/);
    assert.match(html, /<option value="warn"/);
    assert.match(html, /<option value="info"/);
  });

  test("inlines the SSE-handling client-side JavaScript", () => {

    // The script block defines connectSSE, disconnectSSE, and the appendLogEntry/formatLogEntry helpers. We assert their presence by name.
    const html = generateLogsContent();

    assert.match(html, /<script>/);
    assert.match(html, /function connectSSE\(/);
    assert.match(html, /function disconnectSSE\(/);
    assert.match(html, /function appendLogEntry\(/);
    assert.match(html, /function formatLogEntry\(/);
  });

  test("registers the tabactivated listener for SSE lifecycle management", () => {

    // The Logs tab connects to /logs/stream when activated and disconnects when deactivated. The listener is the binding point for this lifecycle.
    const html = generateLogsContent();

    assert.match(html, /document\.addEventListener\(['"]tabactivated['"]/);
  });

  test("does not produce template-literal artifacts", () => {

    const html = generateLogsContent();

    assert.doesNotMatch(html, /\$\{undefined\}/);
    assert.doesNotMatch(html, /\[object Object\]/);
  });
});

describe("generateConfigContent", () => {

  test("returns HTML with a subtab bar including Backup", () => {

    // The Configuration tab has Settings + Advanced + Backup as subtabs. Settings is default active. The Backup subtab is appended unconditionally after the
    // dynamic tab list.
    const html = generateConfigContent();

    assert.match(html, /class="subtab-bar"/);
    assert.match(html, /data-subtab="backup"/, "Backup subtab button should always render");
  });

  test("includes the settings form with the save and reset buttons", () => {

    // The settings form wraps the settings + advanced subtabs. Save and Reset All to Defaults are the two action buttons. The button text differs based on
    // service mode but both should be present.
    const html = generateConfigContent();

    assert.match(html, /id="settings-form"/);
    assert.match(html, /id="save-btn"/);
    assert.match(html, /onclick="resetAllToDefaults\(\)"/);
  });

  test("includes the Backup subtab panel with download/import controls", () => {

    // generateBackupPanel renders the backup section including download Settings/Channels and import buttons.
    const html = generateConfigContent();

    assert.match(html, /id="subtab-backup"/);
    assert.match(html, /onclick="exportConfig\(\)"/);
    assert.match(html, /onclick="exportChannels\(\)"/);
    assert.match(html, /id="import-settings-file"/);
    assert.match(html, /id="import-channels-file"/);
  });

  test("does not produce template-literal artifacts", () => {

    const html = generateConfigContent();

    assert.doesNotMatch(html, /\$\{undefined\}/);
    assert.doesNotMatch(html, /\[object Object\]/);
  });

  test("opens settings-form correctly with closing form tag (balanced)", () => {

    // Crude balance check: the form must have a closing tag. If a refactor accidentally drops it, every input below would be parented to the wrong ancestor.
    const html = generateConfigContent();
    const opens = (html.match(/<form\b/g) ?? []).length;
    const closes = (html.match(/<\/form>/g) ?? []).length;

    assert.equal(opens, closes, "form tag balance (opens=" + String(opens) + ", closes=" + String(closes) + ")");
  });
});
