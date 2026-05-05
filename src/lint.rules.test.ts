/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * lint.rules.test.ts: Unit tests for the project-local ESLint rules defined in eslint.config.mjs. The rules enforce conventions that have caused regressions
 * (test helpers under src/types/, direct submodule imports of testing helpers); a regression in the rule logic itself - a typo in the regex, a path-handling
 * mistake - would silently re-open the door. These tests pin each rule's expected behavior against representative valid and invalid inputs via ESLint's
 * RuleTester, the standard testing harness for custom rules.
 */
import { describe, test } from "node:test";
import { RuleTester } from "eslint";
// @ts-expect-error - eslint.config.mjs has no .d.ts companion, so TS can't infer a type for the named export. RuleTester validates the rule shape at runtime.
import { rules as eslintRules } from "../eslint.config.mjs";

const rules = eslintRules as Record<string, Parameters<RuleTester["run"]>[1]>;

/* RuleTester runs synchronous assertions; failure throws. We wrap each rule's run() inside a node:test test() block so the harness's failures surface as the
 * test runner's failures (with a useful test name). The languageOptions are minimal because these rules don't inspect parsed JavaScript syntax beyond what
 * the parser provides by default (no JSX, no TypeScript-specific node types).
 */
const ruleTester = new RuleTester({

  languageOptions: {

    ecmaVersion: 2024,
    sourceType: "module"
  }
});

describe("prismcast/no-helpers-in-types", () => {

  test("fires on a *.helpers.ts file under src/types/", () => {

    ruleTester.run("no-helpers-in-types", rules["no-helpers-in-types"]!, {

      invalid: [
        {

          code: "export const x = 1;",
          errors: [{ messageId: "forbidden" }],
          filename: "/abs/path/to/src/types/channels.helpers.ts"
        }
      ],
      valid: []
    });
  });

  test("fires on a *.helpers.test.ts file under src/types/", () => {

    ruleTester.run("no-helpers-in-types", rules["no-helpers-in-types"]!, {

      invalid: [
        {

          code: "export const x = 1;",
          errors: [{ messageId: "forbidden" }],
          filename: "/abs/path/to/src/types/channels.helpers.test.ts"
        }
      ],
      valid: []
    });
  });

  test("does NOT fire on a *.helpers.ts file outside src/types/", () => {

    ruleTester.run("no-helpers-in-types", rules["no-helpers-in-types"]!, {

      invalid: [],
      valid: [
        { code: "export const x = 1;", filename: "/abs/path/to/src/streaming/registry.helpers.ts" },
        { code: "export const x = 1;", filename: "/abs/path/to/src/config/userChannels.helpers.ts" },
        { code: "export const x = 1;", filename: "/abs/path/to/src/routes/express.helpers.ts" }
      ]
    });
  });

  test("does NOT fire on a non-helpers file under src/types/", () => {

    // Boundary: a regular type-definition file (channels.ts, profiles.ts) under src/types/ must not trigger - the rule targets the helpers naming convention,
    // not all files in the directory. Code samples are plain JS because RuleTester's default parser doesn't accept TS syntax; the rule is path-based and
    // doesn't inspect the AST beyond Program, so the body content is irrelevant.
    ruleTester.run("no-helpers-in-types", rules["no-helpers-in-types"]!, {

      invalid: [],
      valid: [
        { code: "export const a = 1;", filename: "/abs/path/to/src/types/channels.ts" },
        { code: "export const b = 1;", filename: "/abs/path/to/src/types/index.ts" },
        { code: "export const c = 1;", filename: "/abs/path/to/src/types/selection.test.ts" }
      ]
    });
  });
});

describe("prismcast/testing-helpers-barrel-only", () => {

  test("fires on a direct import of a src/testing/ submodule", () => {

    ruleTester.run("testing-helpers-barrel-only", rules["testing-helpers-barrel-only"]!, {

      invalid: [
        {

          code: "import { silentLog } from '../testing/loggers.helpers.ts';",
          errors: [{ messageId: "barrelOnly" }]
        },
        {

          code: "import { assertSameShape } from '../testing/parity.helpers.ts';",
          errors: [{ messageId: "barrelOnly" }]
        },
        {

          code: "import { assertSameShape } from '../../testing/parity.helpers.ts';",
          errors: [{ messageId: "barrelOnly" }]
        },
        {

          code: "import { silentLog } from '../testing/loggers.helpers.test.ts';",
          errors: [{ messageId: "barrelOnly" }]
        }
      ],
      valid: []
    });
  });

  test("does NOT fire on the barrel import (../testing.helpers.ts)", () => {

    // Boundary: the barrel itself is the canonical entry. Any import path ending in /testing.helpers.ts (no slash after "testing") must pass.
    ruleTester.run("testing-helpers-barrel-only", rules["testing-helpers-barrel-only"]!, {

      invalid: [],
      valid: [
        { code: "import { silentLog } from '../testing.helpers.ts';" },
        { code: "import { silentLog } from '../../testing.helpers.ts';" },
        { code: "import { firstOf } from './testing.helpers.ts';" }
      ]
    });
  });

  test("does NOT fire on imports that don't touch the testing subtree", () => {

    ruleTester.run("testing-helpers-barrel-only", rules["testing-helpers-barrel-only"]!, {

      invalid: [],
      valid: [
        { code: "import { foo } from './bar.ts';" },
        { code: "import { something } from '../config/userConfig.ts';" },
        { code: "import { Page } from 'puppeteer-core';" },
        { code: "import path from 'node:path';" }
      ]
    });
  });

  test("does NOT fire on a similarly-named path that is NOT a testing submodule import", () => {

    // Boundary: a path containing "testing" elsewhere (e.g., "../some-testing-package/foo.ts" or a dependency named e2e-testing) must not match. The rule
    // looks for the literal /testing/<name>.helpers(.test)?.ts suffix; other paths fall through.
    ruleTester.run("testing-helpers-barrel-only", rules["testing-helpers-barrel-only"]!, {

      invalid: [],
      valid: [
        { code: "import { foo } from '../some-testing-package/foo.ts';" },
        { code: "import { foo } from '../testing/foo.ts';" },
        { code: "import { foo } from '../testing/loggers.ts';" }
      ]
    });
  });
});
