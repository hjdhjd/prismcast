/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * lint.clock-port.test.ts: The reference rows for the clock-port completeness rule, run through ESLint's RuleTester under the TypeScript parser so the
 * exempted page-callback shapes can carry the typed parameters the real callbacks carry.
 */
import { describe, test } from "node:test";
import { RuleTester } from "eslint";
// @ts-expect-error - eslint.config.mjs has no .d.ts companion, so TS can't infer a type for the named export. RuleTester validates the rule shape at runtime.
import { rules as eslintRules } from "../eslint.config.mjs";
import tseslint from "typescript-eslint";

const rules = eslintRules as Record<string, Parameters<RuleTester["run"]>[1]>;

/* The TypeScript parser is what lets a fixture carry a typed page callback; the plain-JS default parser the sibling rules use would reject the annotations. The
 * options mirror the module shape of the project's own flat config, whose base sets ecmaVersion "latest"; these rows fix it at 2024.
 */
const ruleTester = new RuleTester({

  languageOptions: {

    ecmaVersion: 2024,
    parser: tseslint.parser,
    sourceType: "module"
  }
});

const rule = rules["clock-port"]!;

describe("prismcast/clock-port", () => {

  test("fires on every direct wall-clock read outside a page callback", () => {

    ruleTester.run("clock-port", rule, {

      invalid: [
        { code: "const t = Date.now();", errors: [{ messageId: "dateNow" }] },
        { code: "const d = new Date();", errors: [{ messageId: "newDate" }] },
        { code: "const p = performance.now();", errors: [{ messageId: "performanceNow" }] },
        { code: "function stamp(): number { return Date.now(); }", errors: [{ messageId: "dateNow" }] },
        { code: "const f = async (): Promise<string> => new Date().toISOString();", errors: [{ messageId: "newDate" }] }
      ],
      valid: []
    });
  });

  test("fires on every platform timer reference outside a page callback, the handle type included", () => {

    // The message names the timer it caught, and a handle held as ReturnType<typeof setTimeout> is the platform timer kept as a type rather than armed.
    ruleTester.run("clock-port", rule, {

      invalid: [
        { code: "setTimeout(() => {}, 5);", errors: [{ data: { name: "setTimeout" }, messageId: "globalTimer" }] },
        { code: "const h = setInterval(() => {}, 5);", errors: [{ data: { name: "setInterval" }, messageId: "globalTimer" }] },
        { code: "declare const h: number; clearTimeout(h);", errors: [{ data: { name: "clearTimeout" }, messageId: "globalTimer" }] },
        { code: "declare const h: number; clearInterval(h);", errors: [{ data: { name: "clearInterval" }, messageId: "globalTimer" }] },
        { code: "const arm = (): void => { setTimeout(() => {}, 5); };", errors: [{ data: { name: "setTimeout" }, messageId: "globalTimer" }] },
        { code: "declare let handle: ReturnType<typeof setTimeout> | null;", errors: [{ data: { name: "setTimeout" }, messageId: "timerHandleType" }] },
        { code: "interface S { readonly timer: ReturnType<typeof setInterval> }", errors: [{ data: { name: "setInterval" }, messageId: "timerHandleType" }] }
      ],
      valid: []
    });
  });

  test("fires on the platform's timeout signal outside a page callback", () => {

    // The composition form reads no time - it only joins signals somebody else armed - so AbortSignal.any stays allowed, alongside the port's own bound and any
    // local method that merely shares the name.
    ruleTester.run("clock-port", rule, {

      invalid: [
        { code: "const s = AbortSignal.timeout(5);", errors: [{ messageId: "abortTimeout" }] },
        { code: "declare function f(init: { signal: AbortSignal }): void; f({ signal: AbortSignal.timeout(5) });", errors: [{ messageId: "abortTimeout" }] },
        { code: "const g = (): AbortSignal => AbortSignal.timeout(5);", errors: [{ messageId: "abortTimeout" }] }
      ],
      valid: [
        { code: "declare const a: AbortSignal; declare const b: AbortSignal; const s = AbortSignal.any([ a, b ]);" },
        { code: "declare function timeoutSignal(ms: number): { signal: AbortSignal }; const s = timeoutSignal(5).signal;" },
        { code: "const local = { timeout: (ms: number): number => ms }; local.timeout(5);" },
        { code: "declare const page: { evaluate<T>(fn: () => T): Promise<T> }; await page.evaluate(() => AbortSignal.timeout(5));" }
      ]
    });
  });

  test("fires on an import of the platform sleep", () => {

    ruleTester.run("clock-port", rule, {

      invalid: [
        { code: "import { setTimeout as sleep } from \"node:timers/promises\";", errors: [{ messageId: "timersImport" }] },
        { code: "import { setImmediate } from \"node:timers/promises\";", errors: [{ messageId: "timersImport" }] }
      ],
      valid: []
    });
  });

  test("does NOT fire on reads and timers that go through the port", () => {

    // The member forms are the port's own surface: a clock's now, delay, and schedule, a registry's keyed timers, a Date built from a supplied instant, and a
    // handle held as the port's Disposable.
    ruleTester.run("clock-port", rule, {

      invalid: [],
      valid: [
        { code: "declare const clock: { now(): number }; const t = clock.now();" },
        { code: "declare const clock: { schedule(cb: () => void, ms: number): void }; clock.schedule(() => {}, 5);" },
        { code: "declare const clock: { delay(ms: number): Promise<void> }; await clock.delay(5);" },
        { code: "declare const timers: { setTimeout(key: string, cb: () => void, ms: number): void }; timers.setTimeout(\"k\", () => {}, 5);" },
        { code: "declare const timers: { clearInterval(key: string): void }; timers.clearInterval(\"k\");" },
        { code: "declare const now: number; const d = new Date(now);" },
        { code: "declare const s: string; const d = new Date(s).getTime();" },
        { code: "const local = { setTimeout: (cb: () => void, ms: number): void => {} }; local.setTimeout(() => {}, 5);" },
        { code: "import { delay } from \"./delay.ts\"; await delay(5);" },
        { code: "declare let handle: Disposable | null;" }
      ]
    });
  });

  test("does NOT fire inside a page callback, however deeply nested", () => {

    // The three page methods take the callback as their first argument; evaluateWithAbort takes it as its second. A helper declared inside the callback is
    // page code too, so the walk continues past it.
    ruleTester.run("clock-port", rule, {

      invalid: [],
      valid: [
        { code: "declare const page: { evaluate<T>(fn: () => T): Promise<T> }; await page.evaluate(() => Date.now());" },
        { code: "declare const page: { evaluateOnNewDocument(fn: (name: string) => void): Promise<void> }; " +
          "await page.evaluateOnNewDocument((name: string): void => { setTimeout(() => { console.log(name, Date.now()); }, 5); });" },
        { code: "declare const page: { evaluateHandle(fn: () => unknown): Promise<unknown> }; await page.evaluateHandle(() => new Date());" },
        { code: "declare const page: unknown; declare function evaluateWithAbort<T>(context: unknown, fn: () => T, args?: unknown[], timeoutMs?: number): Promise<T>; " +
          "await evaluateWithAbort(page, (): number => performance.now(), [], 1000);" },
        { code: "declare const page: { evaluate<T>(fn: () => T): Promise<T> }; " +
          "await page.evaluate(() => { const inner = (): number => Date.now(); const h = setInterval(inner, 5); clearInterval(h); return inner(); });" },
        { code: "declare const page: { evaluate<T>(fn: () => T): Promise<T> }; await page.evaluate(function(): number { return Date.now(); });" }
      ]
    });
  });

  test("fires when the read sits in an argument that is NOT the page callback", () => {

    // Argument position is the boundary: data passed beside the callback runs in Node, and so does a function handed in any other argument slot.
    ruleTester.run("clock-port", rule, {

      invalid: [
        { code: "declare const page: { evaluate(fn: (t: number) => void, t: number): Promise<void> }; await page.evaluate((t: number): void => {}, Date.now());",
          errors: [{ messageId: "dateNow" }] },
        { code: "declare const page: unknown; declare function evaluateWithAbort(context: unknown, fn: () => void, args?: unknown[]): Promise<void>; " +
          "await evaluateWithAbort(page, (): void => {}, [Date.now()]);", errors: [{ messageId: "dateNow" }] },
        { code: "declare const other: { evaluateLater(fn: () => number): void }; other.evaluateLater(() => Date.now());", errors: [{ messageId: "dateNow" }] },
        { code: "declare const page: { evaluate(fn: (f: () => number) => number, f: () => number): Promise<number> }; " +
          "await page.evaluate((f: () => number): number => f(), (): number => Date.now());", errors: [{ messageId: "dateNow" }] },
        { code: "declare function evaluateWithAbort<T>(context: () => number, fn: () => T): Promise<T>; " +
          "await evaluateWithAbort((): number => Date.now(), (): number => 1);", errors: [{ messageId: "dateNow" }] }
      ],
      valid: []
    });
  });
});
