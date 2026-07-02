/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * testing.helpers.ts: Barrel re-export for the cross-cutting testing helper modules under src/testing/. Test files import from this single entry to keep
 * import paths stable as the implementation modules evolve. The thematic submodules under src/testing/ each own one concern:
 *
 *   - cdp.helpers.ts          FakeCdpSession, FakeConnection, makeFakeCdpPage, CapturedCdpCommand, CdpSessionListenerOp (puppeteer CDP stubs)
 *   - cleanup.helpers.ts      closePuppeteerStreamWss, closePuppeteerStreamWssOnIdle
 *   - exec.helpers.ts         FakeExecFileResult, FakeExecFile, bufferOrStringToString, makeExecFileError, execFileFromMap, execFileAlwaysSucceeds
 *   - fn.helpers.ts           noop
 *   - fs.helpers.ts           withTempDir
 *   - loggers.helpers.ts      TestLogger, CapturedLogLine, silentLog, capturingLog
 *   - narrowing.helpers.ts    firstOf, nthOf
 *   - parity.helpers.ts       assertSameShape, declareKeysOf (factory parity checks)
 *   - process.helpers.ts      assertNoUnhandledRejections, expectAt
 *
 * Tests can import from this barrel for the common case, or from a specific submodule when they want to advertise the narrower dependency. Both styles work
 * because the barrel re-exports verbatim.
 */
export type { CapturedCdpCommand, CdpSessionListenerOp } from "./testing/cdp.helpers.ts";
export type { CapturedLogLine, TestLogger } from "./testing/loggers.helpers.ts";
export type { FakeExecFile, FakeExecFileResult } from "./testing/exec.helpers.ts";
export { FakeCdpSession, FakeConnection } from "./testing/cdp.helpers.ts";
export { assertNoUnhandledRejections, expectAt } from "./testing/process.helpers.ts";
export { assertSameShape, declareKeysOf } from "./testing/parity.helpers.ts";
export { bufferOrStringToString, execFileAlwaysSucceeds, execFileFromMap, makeExecFileError } from "./testing/exec.helpers.ts";
export { capturingLog, silentLog } from "./testing/loggers.helpers.ts";
export { closePuppeteerStreamWss, closePuppeteerStreamWssOnIdle } from "./testing/cleanup.helpers.ts";
export { firstOf, nthOf } from "./testing/narrowing.helpers.ts";
export { makeFakeCdpPage } from "./testing/cdp.helpers.ts";
export { noop } from "./testing/fn.helpers.ts";
export { withTempDir } from "./testing/fs.helpers.ts";
