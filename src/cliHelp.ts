/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * cliHelp.ts: Command-line help and environment variable listing for PrismCast.
 */
import { BOOTSTRAP_ENV_VARS, DATA_DIR_VARIABLE } from "./config/paths.ts";
import { CONFIG_METADATA, DEFAULTS, getNestedValue } from "./config/userConfig.ts";
import type { BootstrapEnvVar } from "./config/paths.ts";
import type { SettingMetadata } from "./config/userConfig.ts";

/* The usage text and the environment listing render here, apart from the entry point, because importing the entry point starts the server, and a module that
 * only renders text can be read by a test. Nothing they print is stated here a second time: every setting's description and default come from CONFIG_METADATA
 * and DEFAULTS, and every variable read before config.json from the bootstrap descriptors config/paths.ts exports, so a changed default or a new category
 * reaches the help with no edit to this file.
 */

// The column the usage text aligns each description at.
const DESCRIPTION_COLUMN = 34;

// The heading each CONFIG_METADATA category prints under. A category this table does not name prints under its own key, so the listing never drops one.
const CATEGORY_HEADINGS: Readonly<Record<string, string>> = {

  browser: "Browser",
  channels: "Channels",
  channelsDvr: "Channels DVR",
  hdhr: "HDHomeRun",
  hls: "HLS",
  logging: "Logging",
  paths: "Paths",
  playback: "Playback",
  recovery: "Recovery",
  server: "Server",
  streaming: "Streaming"
};

// The settings whose DEFAULTS value is null because the value in effect is resolved at runtime, with how the help renders that value.
const RUNTIME_DEFAULTS: Readonly<Record<string, string>> = {

  "browser.executablePath": "autodetect",
  "paths.chromeDataDir": "<data-dir>/chromedata",
  "paths.logFile": "<data-dir>/prismcast.log"
};

// The variables the usage text lists as the ones most often set. The environment listing prints every variable.
const COMMON_VARIABLES = [ "AUDIO_BITRATE", "CHROME_BIN", "FRAME_RATE", "HOST", "LOG_MAX_SIZE", "PORT", "PRISMCAST_CHROME_DATA_DIR", "PRISMCAST_DATA_DIR",
  "PRISMCAST_DEBUG", "PRISMCAST_LOG_FILE", "QUALITY_PRESET", "VIDEO_BITRATE" ];

/**
 * What the help prints for an environment variable: its name, the first sentence of its description, and its default as the help renders it.
 */
interface VariableHelp {

  readonly defaultLabel: string;
  readonly name: string;
  readonly summary: string;
}

/**
 * Returns the first sentence of a description. The help prints one line per variable, and the settings form carries the full text.
 * @param description - The description to shorten.
 * @returns The description up to and including its first period followed by a space, or the whole description when it is one sentence.
 */
function firstSentence(description: string): string {

  const periodSpace = description.indexOf(". ");

  return (periodSpace === -1) ? description : description.slice(0, periodSpace + 1);
}

/**
 * Renders a setting's default the way the help prints it: the runtime rendering for a setting resolved at runtime, and otherwise the DEFAULTS value, with its unit
 * when the value is a number that carries one.
 * @param setting - The setting whose default the help prints.
 * @returns The rendered default.
 */
function settingDefault(setting: SettingMetadata): string {

  const runtimeDefault = RUNTIME_DEFAULTS[setting.path];

  if(runtimeDefault !== undefined) {

    return runtimeDefault;
  }

  const value = getNestedValue(DEFAULTS, setting.path);

  return ((typeof value === "number") && setting.unit) ? String(value) + " (" + setting.unit + ")" : String(value);
}

/**
 * Renders the default of the setting a path names, for the usage text's option lines.
 * @param settingPath - The dot-separated setting path.
 * @returns The rendered default, or an empty string when no category declares the path.
 */
function defaultOf(settingPath: string): string {

  const setting = Object.values(CONFIG_METADATA).flat().find((candidate) => candidate.path === settingPath);

  return setting ? settingDefault(setting) : "";
}

/**
 * Returns what the help prints for each variable a category's settings carry, in the order the category declares them.
 * @param settings - The category's settings.
 * @returns The help for each setting that carries an environment variable.
 */
function settingsHelp(settings: readonly SettingMetadata[]): VariableHelp[] {

  return settings.flatMap((setting) => (setting.envVar === null) ? [] :
    [{ defaultLabel: settingDefault(setting), name: setting.envVar, summary: firstSentence(setting.description) }]);
}

/**
 * Returns what the help prints for each bootstrap variable.
 * @param variables - The bootstrap variables.
 * @returns The help for each.
 */
function bootstrapHelp(variables: readonly BootstrapEnvVar[]): VariableHelp[] {

  return variables.map((variable) => ({ defaultLabel: variable.defaultLabel, name: variable.name, summary: firstSentence(variable.description) }));
}

/**
 * Renders the usage text the -h and --help flags print.
 * @returns The usage text, one line per row.
 */
export function renderUsage(): string {

  const summaries = new Map([ ...settingsHelp(Object.values(CONFIG_METADATA).flat()), ...bootstrapHelp(BOOTSTRAP_ENV_VARS) ]
    .map((help): [ string, string ] => [ help.name, help.summary ]));

  return [

    "Usage: prismcast [command] [options]",
    "",
    "Commands:",
    "  service                         Manage PrismCast as a system service",
    "                                  Run 'prismcast service --help' for details",
    "  upgrade                         Upgrade PrismCast to the latest version",
    "                                  Run 'prismcast upgrade --help' for details",
    "",
    "Options:",
    "  -c, --console                   Log to console instead of file (for Docker or debugging)",
    "  -d, --debug                     Enable debug logging (verbose output for troubleshooting)",
    "  -h, --help                      Show this help message",
    "  -p, --port <port>               Set server port (default: " + defaultOf("server.port") + ")",
    "  -v, --version                   Show version number",
    "  --chrome-data-dir <path>        Set Chrome profile data directory (default: " + defaultOf("paths.chromeDataDir") + ")",
    "  --data-dir <path>               Set data directory (default: " + DATA_DIR_VARIABLE.defaultLabel + ")",
    "  --list-env                      List all environment variables",
    "  --log-file <path>               Set log file path (default: " + defaultOf("paths.logFile") + ")",
    "",
    "If no command is specified, starts the PrismCast server.",
    "",
    "Common Environment Variables:",
    ...COMMON_VARIABLES.map((name) => "  " + name.padEnd(DESCRIPTION_COLUMN - 2) + (summaries.get(name) ?? "")),
    "",
    "  Run 'prismcast --list-env' for a complete list of all environment variables."
  ].join("\n");
}

/**
 * Renders the environment variable listing the --list-env flag prints: every CONFIG_METADATA category that holds a setting with an environment variable, the
 * server's first because it is the one most often configured and the rest in the order the metadata declares them, followed by the bootstrap variables.
 * @returns The listing, one line per row.
 */
export function renderEnvironmentVariables(): string {

  const lines = [ "PrismCast Environment Variables", "", "All settings can also be configured via the web UI at /config or config.json.",
    "Priority: CLI flags > environment variables > config.json > defaults." ];

  // Renders one section: its heading, then each variable's name, summary and default, with a blank line between variables.
  const renderSection = (heading: string, entries: readonly VariableHelp[]): void => {

    if(entries.length === 0) {

      return;
    }

    lines.push("", heading + ":");

    for(const [ index, entry ] of entries.entries()) {

      if(index > 0) {

        lines.push("");
      }

      lines.push("  " + entry.name, "    " + entry.summary, "    Default: " + entry.defaultLabel);
    }
  };

  for(const category of [ "server", ...Object.keys(CONFIG_METADATA).filter((key) => key !== "server") ]) {

    renderSection(CATEGORY_HEADINGS[category] ?? category, settingsHelp(CONFIG_METADATA[category] ?? []));
  }

  renderSection("Special", bootstrapHelp(BOOTSTRAP_ENV_VARS));

  return lines.join("\n");
}
