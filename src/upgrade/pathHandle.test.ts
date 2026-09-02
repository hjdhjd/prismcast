/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * pathHandle.test.ts: Unit tests for the platform-aware path value type in pathHandle.ts. The factory accepts an explicit platform override, so a single
 * Linux-hosted test suite can produce both POSIX and Windows handles and verify each query method against the right separator and case-folding semantics. The
 * coverage is organized by method (raw, hasSegmentChain, isUnder, join, parentBefore) and each method exercises both platforms plus the relevant edge cases:
 * empty inputs, degenerate paths, drive boundaries, and case-folding behavior.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { createPathHandle } from "./pathHandle.ts";

describe("createPathHandle", () => {

  test("returns a PathHandle whose raw is byte-faithful to the input", () => {

    // The raw field is the single escape hatch...subprocess cwd handoff and human-facing display both rely on it being identical to the input string. Any
    // canonicalization at construction would silently corrupt those consumers. Exercise both platforms because each has independent reconstruction paths in
    // parentBefore that re-derive raw from a segment split.
    assert.equal(createPathHandle("/Users/me/myproject/node_modules/prismcast/dist/upgrade/detection.js").raw,
      "/Users/me/myproject/node_modules/prismcast/dist/upgrade/detection.js");
    assert.equal(createPathHandle("C:\\Users\\jp\\node_modules\\prismcast\\dist\\detection.js", { platform: "win32" }).raw,
      "C:\\Users\\jp\\node_modules\\prismcast\\dist\\detection.js");
  });

  test("returns a handle whose methods are all callable (shape contract)", () => {

    // Locks the public interface. A future refactor that renamed a method or dropped one would fail this test before any caller noticed at runtime. The shape is
    // small enough to enumerate exhaustively.
    const handle = createPathHandle("/tmp/x.js");

    assert.equal(typeof handle.raw, "string");
    assert.equal(typeof handle.hasSegmentChain, "function");
    assert.equal(typeof handle.isUnder, "function");
    assert.equal(typeof handle.join, "function");
    assert.equal(typeof handle.parentBefore, "function");
  });
});

describe("PathHandle.hasSegmentChain", () => {

  test("matches a single segment that appears in the path (POSIX)", () => {

    const handle = createPathHandle("/Users/me/myproject/node_modules/prismcast/dist/upgrade/detection.js", { platform: "linux" });

    assert.equal(handle.hasSegmentChain("node_modules"), true);
    assert.equal(handle.hasSegmentChain("prismcast"), true);
  });

  test("does not match a single segment that is a substring of a segment but not a segment itself (POSIX)", () => {

    // Substring-style false positives are exactly the bug class this API is designed to prevent. "node_mod" appears inside "node_modules" but is not a segment,
    // so the chain check must return false. A naive .includes() would have returned true and triggered the wrong strategy.
    const handle = createPathHandle("/Users/me/node_modules/prismcast/x.js", { platform: "linux" });

    assert.equal(handle.hasSegmentChain("node_mod"), false);
    assert.equal(handle.hasSegmentChain("prismcast/dist"), false, "a separator-bearing string can never match a single segment");
  });

  test("matches a multi-segment chain when the segments appear consecutively (POSIX)", () => {

    const handle = createPathHandle("/Users/me/my-app/node_modules/prismcast/dist/upgrade/detection.js", { platform: "linux" });

    assert.equal(handle.hasSegmentChain("node_modules", "prismcast"), true);
    assert.equal(handle.hasSegmentChain("my-app", "node_modules", "prismcast"), true);
  });

  test("does not match a multi-segment chain whose segments appear out of order or non-consecutively (POSIX)", () => {

    const handle = createPathHandle("/opt/prismcast/lib/node_modules/foo/x.js", { platform: "linux" });

    assert.equal(handle.hasSegmentChain("node_modules", "prismcast"), false, "wrong order");
    assert.equal(handle.hasSegmentChain("prismcast", "lib"), true, "consecutive segments match in declared order");
    assert.equal(handle.hasSegmentChain("prismcast", "node_modules"), false, "non-consecutive segments do not match");
  });

  test("returns false for an empty chain (no vacuous-truth match)", () => {

    // An empty rest-parameter expansion must not silently match every path. Returning false defensively keeps the API accurate...callers who hand in an empty
    // array (e.g., via a buggy spread) get a clean negative rather than an accidental positive on every install.
    const handle = createPathHandle("/Users/me/anywhere/x.js", { platform: "linux" });

    assert.equal(handle.hasSegmentChain(), false);
  });

  test("returns false when the chain is longer than the path's segment list", () => {

    const handle = createPathHandle("/x.js", { platform: "linux" });

    assert.equal(handle.hasSegmentChain("a", "b", "c", "d"), false);
  });

  test("is case-sensitive on POSIX (Users does not match users)", () => {

    // POSIX filesystems are case-sensitive at the API layer; node:path treats them the same. A case-insensitive comparison on POSIX would mask real layout
    // differences (think macOS APFS with case-sensitive flag enabled, or any Linux system).
    const handle = createPathHandle("/Users/me/node_modules/prismcast/x.js", { platform: "linux" });

    assert.equal(handle.hasSegmentChain("users"), false);
    assert.equal(handle.hasSegmentChain("Node_Modules"), false);
  });

  test("matches case-insensitively on Windows (Node_Modules matches node_modules)", () => {

    // Windows filesystems are case-insensitive at the OS layer. Case-folding both sides of the comparison is the OS-faithful behavior; without it, a user whose
    // npm-installed PrismCast happened to be path-cased differently than the chain literal would silently mis-detect.
    const handle = createPathHandle("C:\\Users\\jp\\AppData\\Roaming\\npm\\Node_Modules\\PRISMCAST\\dist\\x.js", { platform: "win32" });

    assert.equal(handle.hasSegmentChain("node_modules", "prismcast"), true);
    assert.equal(handle.hasSegmentChain("USERS", "JP"), true);
  });

  test("matches a Windows path with backslash separators (the v1.10.2 detection-on-Windows regression class)", () => {

    // hasSegmentChain dispatches through path.win32's separator, so a backslash-delimited Windows path structurally matches the "node_modules" segment
    // chain even though the path never contains a "/node_modules/" substring.
    const handle = createPathHandle("C:\\Users\\jp\\AppData\\Roaming\\npm\\node_modules\\prismcast\\dist\\upgrade\\detection.js", { platform: "win32" });

    assert.equal(handle.hasSegmentChain("node_modules"), true);
    assert.equal(handle.hasSegmentChain("node_modules", "prismcast"), true);
  });
});

describe("PathHandle.isUnder", () => {

  test("returns true for a strict descendant on POSIX", () => {

    const handle = createPathHandle("/usr/local/lib/node_modules/prismcast/dist/x.js", { platform: "linux" });

    assert.equal(handle.isUnder("/usr/local"), true);
    assert.equal(handle.isUnder("/usr/local/lib/node_modules"), true);
  });

  test("returns false when the path equals the prefix exactly (strict-under semantics)", () => {

    // The install detection use case is "is the running file under some directory" - the running file is a JS file, the prefix is a directory, so equality is
    // a degenerate case that should never match. Locking the strict semantic prevents an accidental "this directory IS the prefix" hit from being treated as
    // "this directory is INSTALLED at the prefix."
    const handle = createPathHandle("/usr/local", { platform: "linux" });

    assert.equal(handle.isUnder("/usr/local"), false);
  });

  test("returns false for a sibling path on POSIX", () => {

    const handle = createPathHandle("/usr/local/lib/node_modules/prismcast/x.js", { platform: "linux" });

    assert.equal(handle.isUnder("/usr/local/foo"), false);
    assert.equal(handle.isUnder("/opt/homebrew"), false);
  });

  test("returns false for an empty or relative prefix", () => {

    // Defensive: relative containment is undefined for absolute filesystem paths, and an empty prefix would trivially match everything. The strategy layer
    // depends on this defensive behavior because runCommand may return an empty string for npm prefix queries and the strategy passes it through verbatim.
    const handle = createPathHandle("/usr/local/lib/node_modules/prismcast/x.js", { platform: "linux" });

    assert.equal(handle.isUnder(""), false);
    assert.equal(handle.isUnder("usr/local"), false, "relative prefix");
    assert.equal(handle.isUnder("./local"), false, "dot-relative prefix");
  });

  test("returns true for a strict descendant on Windows with backslash separators", () => {

    // isUnder verifies containment under the npm-reported global prefix, a check distinct from hasSegmentChain's node_modules tree confirmation. Both
    // sides use backslash on Windows, so the comparison succeeds for a Windows-style path.
    const handle = createPathHandle("C:\\Users\\jp\\AppData\\Roaming\\npm\\node_modules\\prismcast\\dist\\x.js", { platform: "win32" });

    assert.equal(handle.isUnder("C:\\Users\\jp\\AppData\\Roaming\\npm"), true);
  });

  test("matches case-insensitively on Windows when the prefix casing differs from the path casing", () => {

    // Windows filesystem semantics are case-insensitive. The classic failure mode is npm's reported prefix using lowercase ("c:\\users\\jp\\appdata\\roaming\\
    // npm") while import.meta.url canonicalizes to uppercase. Both denote the same directory on the OS; the structural check must agree.
    const handle = createPathHandle("C:\\Users\\JP\\AppData\\Roaming\\npm\\node_modules\\prismcast\\x.js", { platform: "win32" });

    assert.equal(handle.isUnder("c:\\users\\jp\\appdata\\roaming\\npm"), true);
  });

  test("returns false when the Windows path is on a different drive than the prefix", () => {

    // Drive boundaries cannot be crossed. Even if every subdirectory below the drive happened to match, the leading drive segment differs and the segment-list
    // comparison rejects cleanly.
    const handle = createPathHandle("D:\\dev\\node_modules\\prismcast\\x.js", { platform: "win32" });

    assert.equal(handle.isUnder("C:\\dev"), false);
  });

  test("normalizes the prefix on Windows so a forward-slash-typed prefix still works", () => {

    // Defensive: a user-typed config (or a CLI argument that came in via a shell that translates separators) could deliver a Windows prefix using forward
    // slashes. path.normalize on win32 converts those to backslashes before the segment split, so the comparison still succeeds.
    const handle = createPathHandle("C:\\Users\\jp\\AppData\\Roaming\\npm\\node_modules\\prismcast\\x.js", { platform: "win32" });

    assert.equal(handle.isUnder("C:/Users/jp/AppData/Roaming/npm"), true);
  });
});

describe("PathHandle.join", () => {

  test("joins child segments with the platform-native separator (POSIX)", () => {

    const handle = createPathHandle("/Users/me/myproject", { platform: "linux" });

    assert.equal(handle.join("package.json"), "/Users/me/myproject/package.json");
    assert.equal(handle.join("src", "index.ts"), "/Users/me/myproject/src/index.ts");
  });

  test("joins child segments with the platform-native separator (Windows)", () => {

    // Identical use case on Windows: the resolver's manifest-path construction must produce a backslash-delimited string that npm install accepts as cwd. Any
    // forward-slash leak here would manifest as a "package.json not found" downstream.
    const handle = createPathHandle("C:\\Users\\jp\\my-app", { platform: "win32" });

    assert.equal(handle.join("package.json"), "C:\\Users\\jp\\my-app\\package.json");
    assert.equal(handle.join("src", "index.ts"), "C:\\Users\\jp\\my-app\\src\\index.ts");
  });

  test("returns the normalized raw path when no segments are supplied", () => {

    // path.join's documented behavior with a single argument is to normalize the input. We rely on this nowhere in production (the resolver always passes at
    // least one segment), but locking the behavior protects callers who might call join() bare in the future and expect a stable result.
    const handle = createPathHandle("/Users/me/myproject", { platform: "linux" });

    assert.equal(handle.join(), "/Users/me/myproject");
  });
});

describe("PathHandle.parentBefore", () => {

  test("returns a handle anchored at the slice ending just before the chain (POSIX)", () => {

    const handle = createPathHandle("/Users/me/my-app/node_modules/prismcast/dist/x.js", { platform: "linux" });
    const parent = handle.parentBefore("node_modules", "prismcast");

    assert.ok(parent, "match should produce a non-null handle");
    assert.equal(parent.raw, "/Users/me/my-app");
  });

  test("returned handle inherits the platform so further queries use the same semantics", () => {

    // parentBefore must not silently switch platforms. A test on win32 confirms the returned handle behaves Windows-flavored when join is called on it...this
    // is the exact composition the npm-local resolver does (parentBefore then join("package.json")) and any platform leak would corrupt the manifest path.
    const winHandle = createPathHandle("C:\\Users\\jp\\my-app\\node_modules\\prismcast\\dist\\x.js", { platform: "win32" });
    const winParent = winHandle.parentBefore("node_modules", "prismcast");

    assert.ok(winParent);
    assert.equal(winParent.raw, "C:\\Users\\jp\\my-app");
    assert.equal(winParent.join("package.json"), "C:\\Users\\jp\\my-app\\package.json");
  });

  test("returns null when the chain does not appear in the path", () => {

    const handle = createPathHandle("/Users/me/dev/prismcast/dist/x.js", { platform: "linux" });

    assert.equal(handle.parentBefore("node_modules", "prismcast"), null);
  });

  test("returns null when the chain matches at position zero (no parent above it)", () => {

    // A path that starts directly with the chain (after the leading separator's empty segment) has no meaningful parent...the install layout where node_modules
    // is at the filesystem root is degenerate and the resolver must skip it cleanly rather than return an empty string as packageDir.
    const handle = createPathHandle("/node_modules/prismcast/dist/x.js", { platform: "linux" });

    assert.equal(handle.parentBefore("node_modules", "prismcast"), null);
  });

  test("returns null when the parent slice is a bare Windows drive letter (degenerate layout)", () => {

    // Symmetric to the POSIX root case. A path like "C:\\node_modules\\prismcast" has a parent slice of "C:" with no trailing separator, which is not an
    // absolute path on Windows (path.win32.isAbsolute("C:") returns false). Returning null here keeps the resolver from passing an unusable cwd to npm install.
    const handle = createPathHandle("C:\\node_modules\\prismcast\\dist\\x.js", { platform: "win32" });

    assert.equal(handle.parentBefore("node_modules", "prismcast"), null);
  });

  test("uses the first occurrence of the chain when the chain appears more than once", () => {

    // Workspace-style layouts can nest node_modules trees. The first-occurrence semantic matches String.prototype.indexOf and preserves the original detection
    // logic's behavior (slice before the first /node_modules/prismcast/). The outermost project root is the right answer for the resolver because that is where
    // the user-facing package.json lives.
    const handle = createPathHandle("/Users/me/outer/node_modules/some-pkg/node_modules/prismcast/dist/x.js", { platform: "linux" });
    const parent = handle.parentBefore("node_modules", "prismcast");

    assert.ok(parent);
    assert.equal(parent.raw, "/Users/me/outer/node_modules/some-pkg", "first occurrence of the chain anchors the parent");
  });

  test("returned POSIX handle preserves the leading slash on the raw reconstruction", () => {

    // Splitting "/Users/me/.../x.js" on "/" produces an empty first segment, which the reconstruction must preserve so the joined parent path is absolute. A
    // bug here would produce "Users/me/my-app" instead of "/Users/me/my-app" and break isUnder checks downstream.
    const handle = createPathHandle("/Users/me/my-app/node_modules/prismcast/dist/x.js", { platform: "linux" });
    const parent = handle.parentBefore("node_modules", "prismcast");

    assert.ok(parent);
    assert.equal(parent.raw.startsWith("/"), true, "POSIX parent path must remain absolute");
  });

  test("returned handle's hasSegmentChain queries follow the inherited platform semantics", () => {

    // End-to-end inheritance check: chain the parent navigation with another structural query on the resulting handle. This is the kind of composition that
    // strategy authors might write naturally; verify the platform-inheritance contract holds across the chain.
    const winHandle = createPathHandle("C:\\Users\\JP\\my-app\\node_modules\\prismcast\\dist\\x.js", { platform: "win32" });
    const winParent = winHandle.parentBefore("node_modules", "prismcast");

    assert.ok(winParent);
    assert.equal(winParent.hasSegmentChain("users", "jp"), true, "case-insensitive matching on the returned win32 handle");

    const posixHandle = createPathHandle("/Users/me/my-app/node_modules/prismcast/dist/x.js", { platform: "linux" });
    const posixParent = posixHandle.parentBefore("node_modules", "prismcast");

    assert.ok(posixParent);
    assert.equal(posixParent.hasSegmentChain("users", "me"), false, "case-sensitive matching on the returned posix handle");
  });
});
