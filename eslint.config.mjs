/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * eslint.config.mjs: Linting configuration for PrismCast.
 */
import hbPluginUtils from "homebridge-plugin-utils/eslint";
import path from "node:path";

/* Project-local ESLint rules. Each rule enforces a convention specific to this codebase that has caused regressions in the past:
 *
 * - no-helpers-in-types: prevents test helpers from being placed under src/types/. The types/ folder is for type definitions only; test helpers belong
 *   adjacent to the production module that constructs the value (e.g., src/streaming/registry.helpers.ts next to src/streaming/registry.ts). Fires on any
 *   file whose path matches src/types/<...>/*.helpers.ts or src/types/<...>/*.helpers.test.ts.
 *
 * - testing-helpers-barrel-only: enforces a single canonical import path for the cross-cutting testing helpers. Tests outside src/testing/ must import from
 *   the barrel (src/testing.helpers.ts), not from individual submodules. The submodules are implementation details; holding callers to the barrel keeps a
 *   single canonical entry point and lets the implementation evolve without rippling through the suite.
 *
 * - clock-port: time is read and timers are armed through the library's Clock port. A direct wall-clock read, a platform timer call, a platform timeout
 *   signal, a platform timer handle held as a type, or an import of node:timers/promises outside a page-context callback is a completeness gap, because it is
 *   a moment a test cannot drive and production cannot redirect. The page-callback exemption is decided by ancestry - a read inside the callback each page
 *   method takes as its first argument, or the one evaluateWithAbort takes as its second, runs in the browser rather than in Node - and the client-script
 *   and helper files are exempt by path.
 *
 * - config-write-entry: the configuration is written only through config/index.ts, by saveConfiguration for the settings surface and writeProcessFields
 *   for the fields the process owns, so the file and the running configuration move together. Outside src/config/index.ts, an import or a
 *   named re-export of the store's writers (mutateConfig, mutateConfigThen) from config/userConfig.ts, a namespace import or a star re-export of that module,
 *   and an assignment, an update or a delete on a member chain rooted at CONFIG are each a write that goes around those operations. The test, helper, and
 *   client-script files are exempt by path, the same list the clock-port rule reads, because a test seeds a file through the store directly.
 *
 * Every project-local rule is exported (named) so unit tests under src/ can import it and exercise the rule logic via ESLint's RuleTester. The default
 * export of this file is the full flat config; the named `rules` export is just the rule definitions, decoupled from homebridge-plugin-utils for
 * testability.
 */
export const rules = {

  "clock-port": {

    create(context) {

      // The callback forms that hand a function to the page: a member call whose property is one of these takes the page function as its first argument, and the
      // project's own evaluateWithAbort takes it as its second. A read inside such a callback runs in the browser, never in Node, so it is never a port consumer.
      const pageMethods = new Set([ "evaluate", "evaluateHandle", "evaluateOnNewDocument" ]);
      const timerGlobals = new Set([ "clearInterval", "clearTimeout", "setInterval", "setTimeout" ]);

      const isPageCallback = (fn) => {

        const call = fn.parent;

        if(!call || (call.type !== "CallExpression")) {

          return false;
        }

        const index = call.arguments.indexOf(fn);
        const callee = call.callee;

        if((callee.type === "MemberExpression") && !callee.computed && (callee.property.type === "Identifier") && pageMethods.has(callee.property.name)) {

          return index === 0;
        }

        return (callee.type === "Identifier") && (callee.name === "evaluateWithAbort") && (index === 1);
      };

      // Walks upward from a node to the file's root. The walk continues past every function boundary, because a helper defined inside a page callback is still
      // page code, and stops only at the first function that is itself a page callback argument.
      const insidePageCallback = (node) => {

        for(let current = node.parent; current; current = current.parent) {

          if(((current.type === "ArrowFunctionExpression") || (current.type === "FunctionExpression")) && isPageCallback(current)) {

            return true;
          }
        }

        return false;
      };

      const report = (node, messageId, data = {}) => {

        if(!insidePageCallback(node)) {

          context.report({ data, messageId, node });
        }
      };

      const isMemberCall = (node, objectName, propertyName) => (node.callee.type === "MemberExpression") && !node.callee.computed &&
        (node.callee.object.type === "Identifier") && (node.callee.object.name === objectName) && (node.callee.property.type === "Identifier") &&
        (node.callee.property.name === propertyName);

      return {

        CallExpression(node) {

          // AbortSignal.any is deliberately absent: it composes signals somebody else armed and reads no time of its own, so it is not a port consumer.
          if(isMemberCall(node, "AbortSignal", "timeout")) {

            report(node, "abortTimeout");
          }

          if(isMemberCall(node, "Date", "now")) {

            report(node, "dateNow");
          }

          if(isMemberCall(node, "performance", "now")) {

            report(node, "performanceNow");
          }
        },

        ImportDeclaration(node) {

          if((node.source.value === "node:timers/promises") || (node.source.value === "timers/promises")) {

            context.report({ messageId: "timersImport", node });
          }
        },

        NewExpression(node) {

          if((node.callee.type === "Identifier") && (node.callee.name === "Date") && (node.arguments.length === 0)) {

            report(node, "newDate");
          }
        },

        // The global timer functions are matched by scope resolution rather than by name, so a registry's or a clock's own setTimeout member never matches: only an
        // identifier that resolves to the global binding, or to nothing at all, is the platform timer. A locally declared binding of the same name is a shadow and
        // stays out of the report. A reference from a type query (ReturnType<typeof setTimeout>) is the platform handle held as a type, and gets its own message.
        "Program:exit"(node) {

          const globalScope = context.sourceCode.getScope(node);
          const flag = (reference) => {

            const identifier = reference.identifier;
            const parent = identifier.parent;

            if((parent?.type === "MemberExpression") && (parent.property === identifier)) {

              return;
            }

            report(identifier, (parent?.type === "TSTypeQuery") ? "timerHandleType" : "globalTimer", { name: identifier.name });
          };

          for(const variable of globalScope.variables) {

            if(timerGlobals.has(variable.name) && (variable.defs.length === 0)) {

              for(const reference of variable.references) {

                flag(reference);
              }
            }
          }

          for(const reference of globalScope.through) {

            if(timerGlobals.has(reference.identifier.name)) {

              flag(reference);
            }
          }
        }
      };
    },
    meta: {

      docs: {

        description: "Time is read and timers are armed through the library's Clock port. Direct wall-clock reads and platform timer calls outside a " +
          "page-context callback are a completeness gap."
      },
      messages: {

        abortTimeout: "Bound a wait through the Clock port (timeoutSignal in utils/delay.ts, which carries the caller's own reason), never AbortSignal.timeout().",
        dateNow: "Read the instant through the Clock port (clock.now() or a now parameter), never Date.now(). A read inside a page.evaluate callback is exempt " +
          "by its ancestry.",
        globalTimer: "Arm and clear timers through the Clock port (clock.schedule, clock.delay, or a TimerRegistry), never the platform's {{name}}.",
        newDate: "Build a Date from an instant the Clock port supplied (new Date(clock.now())), never from the argument-less constructor.",
        performanceNow: "Measure elapsed time through the Clock port (startTimer(clock) or clock.now()), never performance.now().",
        timerHandleType: "Hold an armed timer as the Clock port's Disposable handle or under a TimerRegistry key, never as the platform's {{name}} handle type.",
        timersImport: "Sleep through the Clock port (clock.delay or the delay policy in utils/delay.ts), never node:timers/promises."
      },
      schema: [],
      type: "problem"
    }
  },
  "config-write-entry": {

    create(context) {

      // The configuration layer is the one module that writes the configuration, so it is the one file the rule leaves alone.
      if(context.filename.split(path.sep).join("/").endsWith("src/config/index.ts")) {

        return {};
      }

      const writers = new Set([ "mutateConfig", "mutateConfigThen" ]);

      // A source names the store module when it resolves, against the linted file's own directory, to src/config/userConfig.ts. Resolving the path rather than
      // matching its text catches every relative spelling of the module, and leaves alone a file of the same name in another directory.
      const namesStoreModule = (source) => (typeof source === "string") && source.startsWith(".") &&
        path.resolve(path.dirname(context.filename), source).split(path.sep).join("/").endsWith("src/config/userConfig.ts");

      // A module export name is an identifier, or a string literal in the arbitrary-name form.
      const exportName = (node) => (node.type === "Identifier") ? node.name : node.value;

      // A write target is rooted at CONFIG when walking its member chain down through every object ends at that identifier. The walk reaches a write at any
      // depth and through a computed member, so CONFIG.a, CONFIG.a.b.c and CONFIG["a"].b are each caught.
      const isConfigChain = (node) => {

        if(node.type !== "MemberExpression") {

          return false;
        }

        let root = node;

        while(root.type === "MemberExpression") {

          root = root.object;
        }

        return (root.type === "Identifier") && (root.name === "CONFIG");
      };

      return {

        AssignmentExpression(node) {

          if(isConfigChain(node.left)) {

            context.report({ messageId: "configWrite", node });
          }
        },

        ExportAllDeclaration(node) {

          if(namesStoreModule(node.source.value)) {

            context.report({ messageId: "starExport", node });
          }
        },

        ExportNamedDeclaration(node) {

          if(!node.source || !namesStoreModule(node.source.value)) {

            return;
          }

          for(const specifier of node.specifiers) {

            if(writers.has(exportName(specifier.local))) {

              context.report({ data: { name: exportName(specifier.local) }, messageId: "writerReExport", node: specifier });
            }
          }
        },

        ImportDeclaration(node) {

          if(!namesStoreModule(node.source.value)) {

            return;
          }

          for(const specifier of node.specifiers) {

            if(specifier.type === "ImportNamespaceSpecifier") {

              context.report({ messageId: "namespaceImport", node: specifier });

              continue;
            }

            if((specifier.type === "ImportSpecifier") && writers.has(exportName(specifier.imported))) {

              context.report({ data: { name: exportName(specifier.imported) }, messageId: "writerImport", node: specifier });
            }
          }
        },

        UnaryExpression(node) {

          if((node.operator === "delete") && isConfigChain(node.argument)) {

            context.report({ messageId: "configWrite", node });
          }
        },

        UpdateExpression(node) {

          if(isConfigChain(node.argument)) {

            context.report({ messageId: "configWrite", node });
          }
        }
      };
    },
    meta: {

      docs: {

        description: "The configuration is written only through config/index.ts's saveConfiguration and writeProcessFields. A store writer imported from " +
          "config/userConfig.ts, or a write to CONFIG, anywhere else bypasses them."
      },
      messages: {

        configWrite: "Change the running configuration through config/index.ts's saveConfiguration or writeProcessFields, never by writing to CONFIG directly.",
        namespaceImport: "Import config/userConfig.ts by name, never as a namespace, so its store writers stay out of reach outside config/index.ts.",
        starExport: "Never re-export config/userConfig.ts wholesale, because that carries its store writers past config/index.ts.",
        writerImport: "Write the configuration through config/index.ts's saveConfiguration or writeProcessFields, never through {{name}} imported from " +
          "config/userConfig.ts.",
        writerReExport: "Never re-export {{name}} from config/userConfig.ts, because the configuration is written only through config/index.ts."
      },
      schema: [],
      type: "problem"
    }
  },
  "no-helpers-in-types": {

    create(context) {

      return {

        Program(node) {

          // ESLint 9 exposes the filename via context.filename; older builds expose getFilename(). We support both because the project may lock different
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

// The files the Node-runtime rules skip: the test suites and their helpers, the testing infrastructure, and the landing page's content and client scripts,
// which carry code the browser runs. The clock-port and config-write-entry blocks read this one list.
const nodeRuntimeIgnores = [ "src/**/*.helpers.ts", "src/**/*.test.ts", "src/routes/root/content.ts", "src/routes/root/scripts/**/*.ts", "src/testing/**/*.ts",
  "src/testing.helpers.ts" ];

export default hbPluginUtils({

  allowDefaultProject: ["eslint.config.mjs"],

  /* Project-level ESLint overrides applied after the homebridge-plugin-utils base. The block scoped to the TypeScript sources under src and test defers
   * dot-notation to the TS-aware rule so it stops fighting the tsconfig's noPropertyAccessFromIndexSignature - bracket access on index-signature
   * properties is required by tsc and must be allowed by ESLint. The block scoped to the test files relaxes these rules: describe and test from node:test
   * return Promise<void> by design (no-floating-promises would fire on every test invocation), and tests own their preconditions and use `value!` when
   * reading out fixture-shaped data (no-non-null-assertion). The block scoped to the src types directory enforces the project-local helper-location rule
   * against everything under it. The block scoped to the src and test TypeScript sources, excluding the testing helpers' own implementation directory,
   * enforces barrel-only imports for the testing helpers, since that directory is itself the barrel's implementation and must import from its own submodules.
   * The block scoped to the src TypeScript sources less the Node-runtime ignore list enforces the clock-port rule, so every time read and timer in server code
   * goes through the Clock port. The block with the same scope and the same ignore list enforces the config-write-entry rule, so every configuration write in
   * server code goes through config/index.ts.
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
    },
    {

      files: ["src/**/*.ts"],
      ignores: nodeRuntimeIgnores,
      plugins: { prismcast: prismcastPlugin },
      rules: {

        "prismcast/clock-port": "error"
      }
    },
    {

      files: ["src/**/*.ts"],
      ignores: nodeRuntimeIgnores,
      plugins: { prismcast: prismcastPlugin },
      rules: {

        "prismcast/config-write-entry": "error"
      }
    }
  ],

  js: ["eslint.config.mjs"],
  ts: [ "src/**/*.ts", "test/**/*.ts" ]
});
