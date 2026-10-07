/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * lint.config-write-entry.test.ts: The reference rows for the configuration write-entry rule, run through ESLint's RuleTester under the TypeScript parser, each
 * row linted as a named file so the rule's one exempt path and its resolution of import sources against the file's directory are each exercised.
 */
import { describe, test } from "node:test";
import { RuleTester } from "eslint";
// @ts-expect-error - eslint.config.mjs has no .d.ts companion, so TS can't infer a type for the named export. RuleTester validates the rule shape at runtime.
import { rules as eslintRules } from "../eslint.config.mjs";
import tseslint from "typescript-eslint";

const rules = eslintRules as Record<string, Parameters<RuleTester["run"]>[1]>;

// The project's own sources parse under the TypeScript parser, so the rows do too, with the flat config's ecmaVersion and module shape.
const ruleTester = new RuleTester({

  languageOptions: {

    ecmaVersion: 2024,
    parser: tseslint.parser,
    sourceType: "module"
  }
});

const rule = rules["config-write-entry"]!;

// The files the rows are linted as: the configuration layer the rule exempts, a configuration-module sibling, and a module in another directory.
const CONFIG_LAYER = "/abs/path/to/src/config/index.ts";
const SERVICES = "/abs/path/to/src/config/services.ts";
const HDHR = "/abs/path/to/src/hdhr/index.ts";

describe("prismcast/config-write-entry", () => {

  test("does NOT fire inside the configuration layer, where the configuration's writers live", () => {

    ruleTester.run("config-write-entry", rule, {

      invalid: [],
      valid: [
        { code: "import { mutateConfigThen, readConfig } from \"./userConfig.ts\";", filename: CONFIG_LAYER },
        { code: "declare let CONFIG: { a: { b: number } }; CONFIG.a.b = 1;", filename: CONFIG_LAYER }
      ]
    });
  });

  test("fires on every import or re-export that reaches the store's writers from outside the configuration layer", () => {

    ruleTester.run("config-write-entry", rule, {

      invalid: [
        { code: "import { mutateConfig } from \"./userConfig.ts\";", errors: [{ data: { name: "mutateConfig" }, messageId: "writerImport" }], filename: SERVICES },
        { code: "import { mutateConfigThen as write } from \"./userConfig.ts\";", errors: [{ data: { name: "mutateConfigThen" }, messageId: "writerImport" }],
          filename: SERVICES },
        { code: "import * as store from \"./userConfig.ts\";", errors: [{ messageId: "namespaceImport" }], filename: SERVICES },
        { code: "export { mutateConfig } from \"./userConfig.ts\";", errors: [{ data: { name: "mutateConfig" }, messageId: "writerReExport" }], filename: SERVICES },
        { code: "export * from \"./userConfig.ts\";", errors: [{ messageId: "starExport" }], filename: SERVICES },
        { code: "import { mutateConfig } from \"../config/userConfig.ts\";", errors: [{ data: { name: "mutateConfig" }, messageId: "writerImport" }], filename: HDHR }
      ],
      valid: []
    });
  });

  test("does NOT fire on a reader, or on a writer's name from a module that resolves elsewhere", () => {

    // The source is resolved against the linted file's directory, so "./userConfig.ts" from src/hdhr names src/hdhr/userConfig.ts, which is not the store module,
    // and a rule that matched the source by its file name alone would fail that row.
    ruleTester.run("config-write-entry", rule, {

      invalid: [],
      valid: [
        { code: "import { readConfig } from \"./userConfig.ts\";", filename: SERVICES },
        { code: "import { readConfig } from \"../config/userConfig.ts\";", filename: HDHR },
        { code: "import { mutateConfig } from \"./userConfig.ts\";", filename: HDHR }
      ]
    });
  });

  test("fires on every write to a member chain rooted at CONFIG outside the configuration layer, at any depth", () => {

    // One member deep, three deep and computed are each a row, so a rule that read only the chain's second object would fail one of them.
    ruleTester.run("config-write-entry", rule, {

      invalid: [
        { code: "declare const CONFIG: { a: { b: number } }; CONFIG.a.b = 1;", errors: [{ messageId: "configWrite" }], filename: SERVICES },
        { code: "declare const CONFIG: { a: { b: number } }; CONFIG.a.b += 1;", errors: [{ messageId: "configWrite" }], filename: SERVICES },
        { code: "declare const CONFIG: { a: { b?: number } }; CONFIG.a.b ??= 1;", errors: [{ messageId: "configWrite" }], filename: SERVICES },
        { code: "declare const CONFIG: { a: { b: number } }; CONFIG.a.b++;", errors: [{ messageId: "configWrite" }], filename: SERVICES },
        { code: "declare const CONFIG: { a: { b?: number } }; delete CONFIG.a.b;", errors: [{ messageId: "configWrite" }], filename: SERVICES },
        { code: "declare const CONFIG: { a: number }; CONFIG.a = 1;", errors: [{ messageId: "configWrite" }], filename: SERVICES },
        { code: "declare const CONFIG: { a: { b: { c: number } } }; CONFIG.a.b.c = 1;", errors: [{ messageId: "configWrite" }], filename: SERVICES },
        { code: "declare const CONFIG: { a: { b: number } }; CONFIG[\"a\"].b = 1;", errors: [{ messageId: "configWrite" }], filename: SERVICES }
      ],
      valid: []
    });
  });

  test("does NOT fire on a read of CONFIG", () => {

    ruleTester.run("config-write-entry", rule, {

      invalid: [],
      valid: [
        { code: "declare const CONFIG: { a: { b: number } }; const value = CONFIG.a.b;", filename: SERVICES }
      ]
    });
  });
});
