/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * eslint.config.mjs: Linting configuration for PrismCast.
 */
import hbPluginUtils from "homebridge-plugin-utils/eslint";

/* Project-local ESLint rules. Each rule enforces a convention specific to this codebase that has caused regressions in the past:
 *
 * - no-helpers-in-types: prevents test helpers from being placed under src/types/. The types/ folder is for type definitions only; test helpers belong
 *   adjacent to the production module that constructs the value (e.g., src/streaming/registry.helpers.ts next to src/streaming/registry.ts). Fires on any
 *   file whose path matches src/types/<...>/*.helpers.ts or src/types/<...>/*.helpers.test.ts.
 *
 * - testing-helpers-barrel-only: enforces a single canonical import path for the cross-cutting testing helpers. Tests outside src/testing/ must import from
 *   the barrel (src/testing.helpers.ts), not from individual submodules. The submodules are implementation details; pinning callers to the barrel keeps a
 *   single canonical entry point and lets the implementation evolve without rippling through the suite.
 *
 * Every project-local rule is exported (named) so unit tests under src/ can import it and exercise the rule logic via ESLint's RuleTester. The default
 * export of this file is the full flat config; the named `rules` export is just the rule definitions, decoupled from homebridge-plugin-utils for
 * testability.
 */
export const rules = {

  "no-helpers-in-types": {

    create(context) {

      return {

        Program(node) {

          // ESLint 9 exposes the filename via context.filename; older builds expose getFilename(). We support both because the project may pin different
          // ESLint majors over time.
          const filename = context.filename ?? (typeof context.getFilename === "function" ? context.getFilename() : "");

          if((/\/src\/types\/.*\.helpers(\.test)?\.ts$/).test(filename)) {

            context.report({ messageId: "forbidden", node });
          }
        }
      };
    },
    meta: {

      docs: {

        description: "Test helpers must not live under src/types/. Place them adjacent to the production module that constructs the value."
      },
      messages: {

        forbidden: "Test helpers (*.helpers.ts) and their tests must not live under src/types/. Move this file adjacent to the production module that " +
          "constructs the value (e.g., a ResolvedChannel factory belongs in src/config/userChannels.helpers.ts, not src/types/channels.helpers.ts)."
      },
      schema: [],
      type: "problem"
    }
  },
  "testing-helpers-barrel-only": {

    create(context) {

      return {

        ImportDeclaration(node) {

          const value = node.source.value;

          if(typeof value !== "string") {

            return;
          }

          // Match any import path that ends with /testing/<name>.helpers.ts or /testing/<name>.helpers.test.ts, regardless of how many ../ levels precede
          // it (so the rule fires equally on "../testing/parity.helpers.ts", "../../testing/parity.helpers.ts", etc.). The barrel itself,
          // "../testing.helpers.ts", does NOT match this pattern (no slash after "testing").
          if((/(?:^|\/)testing\/[^/]+\.helpers(\.test)?\.ts$/).test(value)) {

            context.report({ data: { importPath: value }, messageId: "barrelOnly", node });
          }
        }
      };
    },
    meta: {

      docs: {

        description: "Cross-cutting testing helpers must be imported from the barrel (testing.helpers.ts), not from individual src/testing/ submodules."
      },
      messages: {

        barrelOnly: "Import testing helpers from '../testing.helpers.ts' (the barrel), not directly from '{{importPath}}'. The src/testing/ submodules " +
          "are implementation details of the barrel; pinning all callers to the barrel keeps a single canonical entry point and lets the implementation " +
          "evolve without rippling through the suite."
      },
      schema: [],
      type: "problem"
    }
  }
};

const prismcastPlugin = { rules };

export default hbPluginUtils({

  allowDefaultProject: ["eslint.config.mjs"],

  /* Project-level ESLint overrides applied after the homebridge-plugin-utils base. The block scoped to the TypeScript sources under src and test defers
   * dot-notation to the TS-aware rule so it stops fighting the tsconfig's noPropertyAccessFromIndexSignature - bracket access on index-signature
   * properties is required by tsc and must be allowed by ESLint. The block scoped to the test files relaxes two rules: describe and test from node:test
   * return Promise<void> by design (no-floating-promises would fire on every test invocation), and tests own their preconditions and use `value!` when
   * reading out fixture-shaped data (no-non-null-assertion). The block scoped to the src types directory enforces the project-local helper-location rule
   * against everything under it. The block scoped to the src and test TypeScript sources, excluding the testing helpers' own implementation directory,
   * enforces barrel-only imports for the testing helpers, since that directory is itself the barrel's implementation and must import from its own submodules.
   */
  extraConfigs: [
    {

      files: [ "src/**/*.ts", "test/**/*.ts" ],
      rules: {

        "@typescript-eslint/dot-notation": [ "warn", { allowIndexSignaturePropertyAccess: true } ],
        "dot-notation": "off"
      }
    },
    {

      files: [ "src/**/*.test.ts", "test/**/*.test.ts" ],
      rules: {

        "@typescript-eslint/no-floating-promises": "off",
        "@typescript-eslint/no-non-null-assertion": "off"
      }
    },
    {

      files: ["src/types/**/*.ts"],
      plugins: { prismcast: prismcastPlugin },
      rules: {

        "prismcast/no-helpers-in-types": "error"
      }
    },
    {

      files: [ "src/**/*.ts", "test/**/*.ts" ],
      ignores: [ "src/testing/**/*.ts", "src/testing.helpers.ts" ],
      plugins: { prismcast: prismcastPlugin },
      rules: {

        "prismcast/testing-helpers-barrel-only": "error"
      }
    }
  ],

  js: ["eslint.config.mjs"],
  ts: [ "src/**/*.ts", "test/**/*.ts" ]
});
