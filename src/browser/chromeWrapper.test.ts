/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * chromeWrapper.test.ts: Exercise the Docker wrapper with a fake Chrome executable.
 */
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { join } from "node:path";
import { test } from "node:test";
import { tmpdir } from "node:os";

test("Docker Chrome wrapper merges feature switches and preserves other arguments", () => {

  const directory = mkdtempSync(join(tmpdir(), "prismcast-chrome-wrapper-"));

  try {

    const fakeChrome = join(directory, "chrome");
    const wrapper = join(directory, "wrapper.sh");
    const source = readFileSync(new URL("../../scripts/chrome-no-sandbox.sh", import.meta.url), "utf8");

    writeFileSync(fakeChrome, "#!/bin/bash\nprintf '%s\\0' \"$@\"\n", { mode: 0o755 });
    assert.ok(source.includes("exec /usr/bin/google-chrome-stable "));
    writeFileSync(wrapper, source.replace("/usr/bin/google-chrome-stable", "\"" + fakeChrome + "\""));

    const requiredFeatures = [ "VaapiVideoDecoder", "VaapiVideoEncoder", "AcceleratedVideoEncoder", "VaapiIgnoreDriverChecks" ];
    const passthrough = [ "--user-data-dir=/tmp/profile with spaces", "--disable-features=SomeDisabledFeature", "https://example.test/a?x=1&y=2", "" ];
    const cases = [
      { expected: [], features: [] },
      { expected: ["PdfOopif"], features: ["--enable-features=PdfOopif"] },
      { expected: [ "PdfOopif", "OtherFeature:mode/fast" ], features: [ "--enable-features=PdfOopif", "--enable-features=OtherFeature:mode/fast" ] },
      { expected: [], features: ["--enable-features="] }
    ];

    for(const entry of cases) {

      const args = execFileSync("/bin/bash", [ wrapper, passthrough[0]!, ...entry.features, ...passthrough.slice(1) ]).toString().split("\0").slice(0, -1);
      const featureArgs = args.filter(arg => arg.startsWith("--enable-features="));

      assert.deepEqual(featureArgs, ["--enable-features=" + [ ...requiredFeatures, ...entry.expected ].join(",")]);
      assert.deepEqual(args.filter(arg => !arg.startsWith("--enable-features=")), [
        "--no-sandbox", "--disable-setuid-sandbox", "--disable-gpu-sandbox", "--ignore-gpu-blocklist", ...passthrough
      ]);
    }
  } finally {

    rmSync(directory, { force: true, recursive: true });
  }
});
