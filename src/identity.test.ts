/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * identity.test.ts: Unit tests for the project-identity constants in identity.ts. The values are quoted into installed launchd plists, systemd unit files, and
 * Windows Task Scheduler entries on user machines - a silent rename would leave previously-installed services unreferenceable from a freshly-built binary, so we
 * pin the exact strings here as a deliberate tripwire on any future edit.
 */
import { SERVICE_ID, SERVICE_NAME } from "./identity.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("identity constants", () => {

  test("SERVICE_ID is the documented reverse-DNS bundle identifier", () => {

    assert.equal(SERVICE_ID, "com.github.hjdhjd.prismcast");
  });

  test("SERVICE_NAME is the documented human-readable product name", () => {

    assert.equal(SERVICE_NAME, "PrismCast");
  });
});
