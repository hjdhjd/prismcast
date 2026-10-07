/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cliHelp.test.ts: Unit tests for the command-line usage text and the environment variable listing. The renderers return text, so each row reads the lines they
 * render against the owners every value comes from: CONFIG_METADATA for the categories, variables and descriptions, DEFAULTS for each default, and the bootstrap
 * descriptors config/paths.ts exports for the variables read before config.json. A row that compared the text against a copy written here would hold the help
 * to a second source of the very values the module exists to state once.
 */
import { BOOTSTRAP_ENV_VARS, DATA_DIR_VARIABLE } from "./config/paths.ts";
import { CONFIG_METADATA, DEFAULTS, getNestedValue } from "./config/userConfig.ts";
import { describe, test } from "node:test";
import { renderEnvironmentVariables, renderUsage } from "./cliHelp.ts";
import assert from "node:assert/strict";

// Every CONFIG_METADATA setting that carries an environment variable, the population the listing prints.
const VARIABLE_SETTINGS = Object.values(CONFIG_METADATA).flat().flatMap((setting) => (setting.envVar === null) ? [] : [{ envVar: setting.envVar, setting }]);

/**
 * Reads the block the listing prints for a variable: the summary line and the default line that follow its name.
 * @param lines - The listing's lines.
 * @param name - The variable's name.
 * @returns The summary and the default the listing printed, or null when the variable is not listed.
 */
function readEntry(lines: readonly string[], name: string): { defaultLabel: string; summary: string } | null {

  const index = lines.indexOf("  " + name);

  if(index === -1) {

    return null;
  }

  return { defaultLabel: (lines[index + 2] ?? "").replace(/^ {4}Default: /, ""), summary: (lines[index + 1] ?? "").trim() };
}

describe("renderEnvironmentVariables", () => {

  test("prints a section for every category that holds a variable, the Channels DVR section among them, with every variable under its own category", () => {

    const lines = renderEnvironmentVariables().split("\n");

    for(const [ category, settings ] of Object.entries(CONFIG_METADATA)) {

      const names = settings.map((setting) => setting.envVar).filter((envVar) => envVar !== null);

      if(names.length === 0) {

        continue;
      }

      // A section is the heading line that precedes its first variable by exactly one line.
      const first = lines.indexOf("  " + (names[0] ?? ""));

      assert.ok(first > 0, "the " + category + " category's first variable is listed");
      assert.match(lines[first - 1] ?? "", /^\S.*:$/, "the " + category + " category's variables follow a heading of their own");

      for(const name of names) {

        assert.ok(lines.includes("  " + name), name + " is listed");
      }
    }

    assert.ok(lines.includes("Channels DVR:"), "the Channels DVR category prints under its heading");
    assert.ok(lines.includes("  CHANNELS_DVR_PORT"), "the Channels DVR port's variable is listed");
  });

  test("every default the listing prints is the DEFAULTS value, and each summary opens its setting's description", () => {

    const lines = renderEnvironmentVariables().split("\n");

    for(const { envVar, setting } of VARIABLE_SETTINGS) {

      const entry = readEntry(lines, envVar);
      const expected = String(getNestedValue(DEFAULTS, setting.path));

      assert.ok(entry, envVar + " is listed");
      assert.ok(setting.description.startsWith(entry.summary), envVar + "'s summary is the opening of its description: " + entry.summary);

      // A setting resolved at runtime holds null in DEFAULTS, and the listing describes where its value comes from rather than printing the null.
      if(expected === "null") {

        assert.notEqual(entry.defaultLabel, "null", envVar + " describes its runtime default");

        continue;
      }

      assert.equal(entry.defaultLabel.split(" (")[0], expected, envVar + " prints the DEFAULTS value");
    }
  });

  test("prints the server's section first and the bootstrap variables last, each from its descriptor", () => {

    const lines = renderEnvironmentVariables().split("\n");
    const headings = lines.filter((line) => /^\S.*:$/.test(line));

    assert.equal(headings[0], "Server:", "the server's section prints first");
    assert.equal(headings.at(-1), "Special:", "the bootstrap variables print last");

    for(const variable of BOOTSTRAP_ENV_VARS) {

      assert.deepEqual(readEntry(lines, variable.name), { defaultLabel: variable.defaultLabel, summary: variable.description },
        variable.name + " prints its descriptor's description and default");
    }
  });
});

describe("renderUsage", () => {

  test("every option default reads its owner", () => {

    const lines = renderUsage().split("\n");

    assert.ok(lines.some((line) => line.startsWith("  -p, --port <port>") && line.endsWith("(default: " + String(DEFAULTS.server.port) + ")")),
      "the port option prints the DEFAULTS port");
    assert.ok(lines.some((line) => line.startsWith("  --data-dir <path>") && line.endsWith("(default: " + DATA_DIR_VARIABLE.defaultLabel + ")")),
      "the data directory option prints the bootstrap descriptor's default");
  });

  test("each common variable prints the opening sentence of its owner's description", () => {

    const lines = renderUsage().split("\n");
    const owners = new Map([ ...VARIABLE_SETTINGS.map(({ envVar, setting }): [ string, string ] => [ envVar, setting.description ]),
      ...BOOTSTRAP_ENV_VARS.map((variable): [ string, string ] => [ variable.name, variable.description ]) ]);
    const start = lines.indexOf("Common Environment Variables:");
    const common = lines.slice(start + 1, lines.indexOf("", start));

    assert.ok(common.length > 0, "the usage text lists common variables");

    for(const line of common) {

      const [ name = "", ...rest ] = line.trim().split(/\s+/);
      const summary = rest.join(" ");
      const description = owners.get(name);

      assert.ok(description, name + " is a variable a configuration owner declares");
      assert.ok((summary.length > 0) && description.startsWith(summary), name + " prints the opening of its description: " + summary);
    }
  });
});
