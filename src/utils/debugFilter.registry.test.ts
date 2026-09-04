/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * debugFilter.registry.test.ts: Holds DEBUG_CATEGORIES equal to the set of categories the source actually uses. The /debug page renders one checkbox per
 * registry entry, so the two directions fail in different ways: a category the source emits but the registry omits cannot be switched on from the page at
 * all, and a registry entry nothing uses renders a checkbox that does nothing. Keeping the two in step is the kind of thing a reviewer is expected to
 * remember, and remembering is what fails, so the rows below walk the source and read the answer out of it instead.
 *
 * The walk reads three shapes, because a category reaches the filter by three routes. Most emitters name it in the first argument of a .debug() call, on
 * whichever logger they hold - the module logger, a stream-bound one, an injected one. Two modules receive it instead through a property, logCategory or
 * debugCategory, and emit under a name they do not lexically contain: the manifest interceptor hands one to the HLS playlist observer, and the Cox and
 * Xfinity modules hand theirs to the shared Comcast Polymer factory. The rest are feature gates rather than log filters, and name their category in an
 * isCategoryEnabled() call. What makes the walk exact is that a category is always written as a string literal, either at its emitter or at the property
 * that carries it to one.
 */
import { describe, test } from "node:test";
import { readFile, readdir } from "node:fs/promises";
import { DEBUG_CATEGORIES } from "./debugFilter.ts";
import assert from "node:assert/strict";
import path from "node:path";

// The source root, resolved from this file so the walk does not depend on the directory the runner was started from.
const SOURCE_ROOT = path.join(import.meta.dirname, "..");

// Files that quote category names without using them: tests carry them as fixtures and filter patterns, helpers and declaration files carry none.
const EXCLUDED_SUFFIXES = [ ".d.ts", ".helpers.ts", ".test.ts" ];

// The three shapes a category is written in, each carrying the literal in its first capture group.
const CATEGORY_PATTERNS = [
  /\.debug\(\s*"([^"]+)"/g,
  /\b(?:debugCategory|logCategory)\s*:\s*"([^"]+)"/g,
  /\bisCategoryEnabled\(\s*"([^"]+)"\s*\)/g
];

/**
 * Walks the source tree and collects every debug category literal it carries.
 * @returns A map from category to the source files, relative to the source root, that name it.
 */
async function collectUsedCategories(): Promise<Map<string, string[]>> {

  const entries = await readdir(SOURCE_ROOT, { recursive: true, withFileTypes: true });
  const sourceFiles = entries.filter((entry) => entry.isFile() && entry.name.endsWith(".ts") &&
    !EXCLUDED_SUFFIXES.some((suffix) => entry.name.endsWith(suffix))).map((entry) => path.join(entry.parentPath, entry.name));

  // The files are read together rather than one after another: the walk covers the whole tree, and the reads are independent.
  const contents = await Promise.all(sourceFiles.map(async (filePath) => readFile(filePath, "utf-8")));
  const used = new Map<string, string[]>();

  for(const [ index, text ] of contents.entries()) {

    const relativePath = path.relative(SOURCE_ROOT, sourceFiles[index] ?? "");

    for(const pattern of CATEGORY_PATTERNS) {

      for(const match of text.matchAll(pattern)) {

        const category = match[1];

        if(category === undefined) {

          continue;
        }

        used.set(category, [ ...(used.get(category) ?? []), relativePath ]);
      }
    }
  }

  return used;
}

describe("DEBUG_CATEGORIES against the categories the source uses", () => {

  test("registers every category the source emits, gates on, or hands to an emitter", async () => {

    const used = await collectUsedCategories();
    const registered = new Set(DEBUG_CATEGORIES.map((entry) => entry.category));
    const unregistered = Array.from(used.entries()).filter(([category]) => !registered.has(category))
      .map(([ category, files ]) => category + " (" + files.join(", ") + ")");

    assert.deepEqual(unregistered, [], "a category the source uses has no DEBUG_CATEGORIES entry, so the /debug page cannot switch it on");
  });

  test("carries no category the source never emits, gates on, or hands to an emitter", async () => {

    const used = await collectUsedCategories();
    const unused = DEBUG_CATEGORIES.map((entry) => entry.category).filter((category) => !used.has(category));

    assert.deepEqual(unused, [], "a DEBUG_CATEGORIES entry is used nowhere, so the /debug page renders a checkbox that toggles nothing");
  });
});
