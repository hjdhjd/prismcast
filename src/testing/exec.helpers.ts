/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * exec.helpers.ts: Test helpers that model the contract of node:child_process's promisified execFile so any test that injects an execFile-shaped function into
 * production code can build a fake against the real shape. They serve every consumer of the GeneratorIO/UpgradeContext-style adapter pattern - and any future
 * adapter that wraps a subprocess - so they live here as cross-cutting test infrastructure rather than buried in a single test file. The shape: success returns
 * a `{ stdout, stderr }` object with utf8 strings; failure throws an Error carrying `.stdout` and `.stderr` matching whatever encoding the test fixture authored
 * (string for the real default, Buffer for `encoding: "buffer"` callers). Production diagnostic-enrichment helpers like runAndSurfaceStderr unpack both branches.
 */

/**
 * The result shape passed to execFileFromMap entries. shouldThrow drives the success-vs-failure decision; stderr/stdout populate the result or the thrown
 * Error's properties, accepting Buffer or string so tests can exercise both branches of unpacking helpers.
 */
export interface FakeExecFileResult {

  shouldThrow?: boolean;
  stderr?: Buffer | string;
  stdout?: Buffer | string;
}

/**
 * Signature of an execFile fake. Matches what production code expects from promisified node:child_process.execFile (with the default utf8 encoding): success
 * returns string stdout/stderr; failure throws an Error carrying .stdout and .stderr.
 */
export type FakeExecFile = (file: string, args: string[]) => Promise<{ stderr: string; stdout: string }>;

/**
 * Normalizes a Buffer-or-string-or-undefined to a string. Used internally by the execFile helpers so tests can author fixture data in either encoding without
 * worrying about the conversion at the call site.
 * @param value - The value to normalize.
 * @returns The utf8-decoded string (empty when value is undefined).
 */
export function bufferOrStringToString(value: Buffer | string | undefined): string {

  if(value === undefined) {

    return "";
  }

  return Buffer.isBuffer(value) ? value.toString("utf8") : value;
}

/**
 * Builds a thrown-Error in the shape real promisified execFile produces on a non-zero exit. The Error carries .stdout and .stderr in whatever encoding the
 * test fixture authored - string (real default) or Buffer (encoding: "buffer" config). Diagnostic-enrichment helpers downstream unpack both branches.
 * @param message - The Error's message.
 * @param stderr - The child's stderr output. String or Buffer; both shapes are valid execFile failure shapes.
 * @param stdout - The child's stdout output. String or Buffer; both shapes are valid execFile failure shapes.
 * @returns The Error with .stderr and .stdout populated.
 */
export function makeExecFileError(message: string, stderr: Buffer | string, stdout: Buffer | string): Error {

  const error = new Error(message) as Error & { stderr: Buffer | string; stdout: Buffer | string };

  error.stderr = stderr;
  error.stdout = stdout;

  return error;
}

/**
 * Builds an execFile implementation from a keyed map of "file args.join(' ')" → FakeExecFileResult. Strict by default: unknown commands throw a "no result
 * configured" error so test setups that miss a command surface immediately rather than silently no-op'ing. This is the typical execFile shape for tests of
 * launchctl/systemctl/etc. invocations whose argv is fixed and known up front.
 * @param map - The keyed result map. Keys are "file args.join(' ')" strings.
 * @returns A FakeExecFile that drives responses from the map.
 */
export function execFileFromMap(map: Record<string, FakeExecFileResult>): FakeExecFile {

  /* The returned function is async because promise-function-async requires it and the codebase consistently prefers async for promise-returning functions.
   * No await is needed - failure paths use throw (which async automatically converts to a Promise rejection) - so require-await is suppressed inline; the async
   * keyword's value here is the throw-becomes-rejection semantics, not parallelism.
   */
  // eslint-disable-next-line @typescript-eslint/require-await
  return async (file: string, args: string[]): Promise<{ stderr: string; stdout: string }> => {

    const key = file + " " + args.join(" ");
    const result = map[key];

    if(!result) {

      throw makeExecFileError("fake-exec-fail: no result configured for " + key, "", "");
    }

    if(result.shouldThrow) {

      throw makeExecFileError("fake-exec-fail: " + key, result.stderr ?? "", result.stdout ?? "");
    }

    return { stderr: bufferOrStringToString(result.stderr), stdout: bufferOrStringToString(result.stdout) };
  };
}

/**
 * Builds an execFile implementation that returns success for every invocation regardless of file/args. Used by tests where the argv strings are too dynamic
 * to key reliably (e.g., generators that build PowerShell scripts whose body content depends on runtime data) but the response shape is uniform.
 * @param stdout - Optional stdout content (Buffer or string). Defaults to empty string.
 * @param stderr - Optional stderr content. Defaults to empty string.
 * @returns A FakeExecFile that returns the same success result on every call.
 */
export function execFileAlwaysSucceeds(stdout: Buffer | string = "", stderr: Buffer | string = ""): FakeExecFile {

  // Encoding normalization happens once at factory time so each invocation can return the same precomputed result without recomputing.
  const resolved = { stderr: bufferOrStringToString(stderr), stdout: bufferOrStringToString(stdout) };

  // Async to satisfy the codebase's promise-function-async preference; require-await is suppressed because the function intentionally has no await - the
  // returned value is fully precomputed at factory time.
  // eslint-disable-next-line @typescript-eslint/require-await
  return async (): Promise<{ stderr: string; stdout: string }> => resolved;
}
