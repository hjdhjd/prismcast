/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * commands.status.test.ts: Unit tests for handleStatus under literal-context wiring (synthetic ServiceContext capturing stdout/stderr, returning install state +
 * stream counts on demand). Default-context status tests (the not-installed and clean-system flows) live in commands.test.ts; install/uninstall literal lives in
 * commands.install.test.ts; runtime lifecycle handlers and dispatch helpers literal live in commands.lifecycle.test.ts.
 */
import { describe, test } from "node:test";
import { makeContextHarness, makeFakeGenerator } from "./commands.helpers.ts";
import type { Nullable } from "../types/index.ts";
import type { StalePathResult } from "./generators.ts";
import type { StreamsResponse } from "./commands.ts";
import assert from "node:assert/strict";
import { handleStatus } from "./commands.ts";

describe("handleStatus (literal context)", () => {

  test("not-installed status: shows Yes/No flags and an install hint", async () => {

    const { context, stdout } = makeContextHarness();

    const code = await handleStatus(context);

    assert.equal(code, 0);

    const text = stdout.join("\n");

    assert.match(text, /PrismCast Service Status/);
    assert.match(text, /Installed:\s+No/);
    assert.match(text, /Running:\s+No/);
    assert.match(text, /service install.*to install the service/);
  });

  test("unsupported platform: shows the 'Not available' banner", async () => {

    const { context, stdout } = makeContextHarness({ generator: null, platform: "openbsd" });

    const code = await handleStatus(context);

    assert.equal(code, 0);
    assert.match(stdout.join("\n"), /Service support: Not available/);
    assert.match(stdout.join("\n"), /not supported on this platform/);
  });

  test("installed and running with active streams shows the stream list", async () => {

    const generator = makeFakeGenerator({ installed: true, running: true });
    const fakeStreams: StreamsResponse = {

      count: 2,
      limit: 4,
      streams: [
        { channel: "ABC", duration: 65, id: 1, showName: "Eyewitness News", url: "https://example.com/abc/stream.m3u8" },
        { channel: "NBC", duration: 30, id: 2, showName: "", url: "https://www.nbc.com/live" }
      ]
    };
    const { context, stdout } = makeContextHarness({

      fetchActiveStreams: async (): Promise<StreamsResponse> => fakeStreams,
      generator
    });

    const code = await handleStatus(context);

    assert.equal(code, 0);

    const text = stdout.join("\n");

    assert.match(text, /Installed:\s+Yes/);
    assert.match(text, /Running:\s+Yes/);
    assert.match(text, /Active streams:\s+2\/4/);
    assert.match(text, /ABC.*Eyewitness News/);
    assert.match(text, /NBC/);
  });

  test("running with zero streams shows '0/limit' without iterating an empty list", async () => {

    const generator = makeFakeGenerator({ installed: true, running: true });
    const { context, stdout } = makeContextHarness({

      fetchActiveStreams: async (): Promise<StreamsResponse> => ({ count: 0, limit: 4, streams: [] }),
      generator
    });

    const code = await handleStatus(context);

    assert.equal(code, 0);
    assert.match(stdout.join("\n"), /Active streams:\s+0\/4/);
  });

  test("running but server unreachable shows the '(server not responding)' fallback", async () => {

    const generator = makeFakeGenerator({ installed: true, running: true });
    const { context, stdout } = makeContextHarness({

      fetchActiveStreams: async (): Promise<Nullable<StreamsResponse>> => null,
      generator
    });

    const code = await handleStatus(context);

    assert.equal(code, 0);
    assert.match(stdout.join("\n"), /Active streams:\s+\(server not responding\)/);
  });

  test("installed with stale paths emits the regenerate-on-restart warning", async () => {

    const generator = makeFakeGenerator({ installed: true, running: false });
    const { context, stderr } = makeContextHarness({

      detectStalePaths: (): Nullable<StalePathResult> => ({ entryPoint: "/old/dist/index.js", nodePath: "/old/node", stale: true }),
      generator
    });

    const code = await handleStatus(context);

    assert.equal(code, 0);

    const errorText = stderr.join("\n");

    assert.match(errorText, /Service file contains stale paths/);
    assert.match(errorText, /Run 'prismcast service restart'/);
  });

  test("falls back to URL hostname when channel field is missing on a stream", async () => {

    // The status display extracts a hostname when channel is null. www. prefix is stripped. Malformed URLs become "Stream <id>".
    const generator = makeFakeGenerator({ installed: true, running: true });
    const { context, stdout } = makeContextHarness({

      fetchActiveStreams: async (): Promise<StreamsResponse> => ({

        count: 2,
        limit: 4,
        streams: [
          { channel: null, duration: 0, id: 5, showName: "", url: "https://www.example.com/path" },
          { channel: null, duration: 0, id: 7, showName: "", url: "not-a-url" }
        ]
      }),
      generator
    });

    await handleStatus(context);

    const text = stdout.join("\n");

    assert.match(text, /example\.com/, "www. prefix stripped, hostname shown");
    assert.match(text, /Stream 7/, "malformed URL fell back to 'Stream <id>'");
  });
});

