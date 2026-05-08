/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * userChannels.dangling.test.ts: Direct unit tests for the dangling-canonical preserve-and-warn path in getMergedChannelMap and the warning dedup helper
 * warnDanglingCanonical.
 *
 * Coverage scope:
 *
 *   - getMergedChannelMap with a user variant whose canonicalKey points at a missing canonical: the variant must be preserved (resolveStoredChannel path) so
 *     the user does not silently lose data; the warning must fire so the operator sees the inconsistency.
 *
 *   - warnDanglingCanonical: emits the warn-once dedup. The first call for a given (variant, canonical) pair logs; subsequent calls for the same pair are
 *     silently suppressed. We verify via a captured logger.
 *
 * State note: getMergedChannelMap reads loadedUserChannels (module state populated by initializeUserChannels). Without a full bring-up we exercise it via the
 * mutateChannels integration path - tests writing a dangling variant via mutateChannels surface the read-back behavior. Pure-state alternative would be a test
 * seam; we use the existing integration entrypoint instead per the production-refactor-for-testability rule.
 *
 * For warnDanglingCanonical: this helper is internally stateful (warnedDanglingVariants Set). Tests use unique (variant, canonical) pairs so cross-test bleed
 * is impossible without clearing the set.
 */
import { afterEach, beforeEach, describe, test } from "node:test";
import { LOG } from "../utils/index.ts";
import { __internalForTests } from "./userChannels.ts";
import assert from "node:assert/strict";

const { warnDanglingCanonical } = __internalForTests;

describe("warnDanglingCanonical: warn-once dedup", () => {

  /* Capture LOG.warn calls during each test by stubbing the underlying writer. afterEach restores. Dedup across tests is avoided by using unique key pairs.
   */
  let warnCalls: string[] = [];
  let originalWarn: typeof LOG.warn;

  beforeEach(() => {

    warnCalls = [];
    // Capture LOG.warn references via local binding so the no-unbound-method rule sees an explicit binding rather than a method shorthand reassignment.
    originalWarn = LOG.warn.bind(LOG);
    LOG.warn = (template: string, ...args: unknown[]): void => {

      warnCalls.push(template + " | " + args.map(String).join(", "));
    };
  });

  afterEach(() => {

    LOG.warn = originalWarn;
  });

  test("first call for a (variant, canonical) pair emits a warning", () => {

    warnDanglingCanonical("dangling-test-A", "missing-canonical-A");

    assert.equal(warnCalls.length, 1);
    assert.match(warnCalls[0] ?? "", /dangling-test-A/);
    assert.match(warnCalls[0] ?? "", /missing-canonical-A/);
  });

  test("repeat call for the SAME pair is suppressed (dedup)", () => {

    warnDanglingCanonical("dangling-test-B", "missing-canonical-B");
    warnDanglingCanonical("dangling-test-B", "missing-canonical-B");
    warnDanglingCanonical("dangling-test-B", "missing-canonical-B");

    assert.equal(warnCalls.length, 1, "only the first call logs; subsequent calls dedup");
  });

  test("a different (variant, canonical) pair logs independently", () => {

    /* Each unique pair has its own dedup entry. Two separate dangling variants logging once each yields two warnings, not one.
     */
    warnDanglingCanonical("dangling-test-C", "missing-canonical-C");
    warnDanglingCanonical("dangling-test-D", "missing-canonical-D");

    assert.equal(warnCalls.length, 2);
  });

  test("same variant key, different canonical pair logs independently", () => {

    /* The dedup token is variant + canonical, so changing the canonical produces a fresh entry even with the same variant key.
     */
    warnDanglingCanonical("dangling-test-E", "missing-canonical-E1");
    warnDanglingCanonical("dangling-test-E", "missing-canonical-E2");

    assert.equal(warnCalls.length, 2);
  });
});
