/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * bootSession.test.ts: Unit tests for the boot-session port. The orchestrator's branches are exhaustively covered against context literals; the platform-specific
 * adapter is exercised via parsePid1StartTicks against synthetic /proc/<pid>/stat payloads. Default-context smoke tests confirm the snapshot semantics: repeated
 * calls without an injected ctx must return the same identifier for the lifetime of the process.
 */
import { describe, test } from "node:test";
import type { BootSessionContext } from "./bootSession.ts";
import assert from "node:assert/strict";
import { getBootSessionId } from "./bootSession.ts";
import { parsePid1StartTicks } from "./bootSession.context.ts";

describe("getBootSessionId orchestrator", () => {

  test("outside a container, returns the host boot minute as a string", () => {

    const ctx: BootSessionContext = {

      containerInstanceTag: () => null,
      hostBootMinute: () => 42,
      inContainer: () => false
    };

    assert.equal(getBootSessionId(ctx), "42");
  });

  test("inside a container with an instance tag, returns 'hostBootMinute::tag'", () => {

    const ctx: BootSessionContext = {

      containerInstanceTag: () => "1234567890",
      hostBootMinute: () => 42,
      inContainer: () => true
    };

    assert.equal(getBootSessionId(ctx), "42::1234567890");
  });

  test("inside a container without an instance tag, falls back to the host boot minute alone", () => {

    // Container detection succeeded but /proc/1/stat could not be read. The orchestrator's null-guard returns the host portion alone rather than composing
    // a degenerate "42::null" identifier.
    const ctx: BootSessionContext = {

      containerInstanceTag: () => null,
      hostBootMinute: () => 42,
      inContainer: () => true
    };

    assert.equal(getBootSessionId(ctx), "42");
  });

  test("different host boot minutes (reboot) yield different identifiers", () => {

    const before: BootSessionContext = {

      containerInstanceTag: () => null,
      hostBootMinute: () => 100,
      inContainer: () => false
    };

    const after: BootSessionContext = {

      containerInstanceTag: () => null,
      hostBootMinute: () => 200,
      inContainer: () => false
    };

    assert.notEqual(getBootSessionId(before), getBootSessionId(after));
  });

  test("different container instance tags within the same host boot yield different identifiers", () => {

    // This is the Docker-restart case. Host boot does not change, but each container instance produces its own PID 1 starttime.
    const first: BootSessionContext = {

      containerInstanceTag: () => "100",
      hostBootMinute: () => 50,
      inContainer: () => true
    };

    const second: BootSessionContext = {

      containerInstanceTag: () => "200",
      hostBootMinute: () => 50,
      inContainer: () => true
    };

    assert.notEqual(getBootSessionId(first), getBootSessionId(second));
  });

  test("same context inputs yield the same identifier across calls", () => {

    // Equality semantics: the identifier is opaque, but equal contexts must produce equal identifiers so callers can compare for "same session" reliably.
    const ctx: BootSessionContext = {

      containerInstanceTag: () => "abc",
      hostBootMinute: () => 7,
      inContainer: () => true
    };

    assert.equal(getBootSessionId(ctx), getBootSessionId(ctx));
  });
});

describe("default context snapshot semantics", () => {

  test("the default context returns a non-empty identifier", () => {

    // Smoke test against the live default context. Verifies the module loaded without throwing and returns a usable string. No comparison against an expected
    // value because the host boot minute is environment-dependent.
    const id = getBootSessionId();

    assert.equal(typeof id, "string");
    assert.ok(id.length > 0);
  });

  test("repeated calls without an injected ctx return the same identifier", () => {

    // The default ctx is frozen at module load (createDefaultBootSessionContext snapshots host boot time and container instance tag once). Successive calls
    // must therefore observe the same identifier even if the system clock drifts mid-process. This invariant is what runtimeIdentity relies on.
    const first = getBootSessionId();
    const second = getBootSessionId();

    assert.equal(first, second);
  });
});

describe("parsePid1StartTicks", () => {

  test("extracts field 22 from a representative /proc/1/stat payload", () => {

    // Constructed to mirror a real Linux /proc/1/stat shape, where the comm is followed by the state character and then the numeric fields. The comm contains an embedded
    // paren ("(bash") to exercise the last-paren anchor. Field 22 (starttime, in clock ticks since boot) is 5678; everything else is filler that the parser must skip.
    const raw = "1 ((bash) S 0 1 1 0 -1 4194304 0 0 0 0 0 0 0 0 20 0 1 0 5678 0 0 18446744073709551615 1 1 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n";

    assert.equal(parsePid1StartTicks(raw), "5678");
  });

  test("returns null when there is no closing paren", () => {

    assert.equal(parsePid1StartTicks("garbage"), null);
  });

  test("returns null when the post-paren payload has fewer than 20 fields", () => {

    // Only a handful of fields after the comm - field 22 is unreachable. The parser refuses to guess.
    assert.equal(parsePid1StartTicks("1 (init) S 0 1\n"), null);
  });
});
