/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * configLeaves.helpers.ts: The configuration's leaf paths, for the tests that hold every leaf to a reactivity class.
 */
import { DEFAULTS } from "../config/userConfig.ts";
import { isPlainObject } from "../utils/index.ts";

/**
 * Lists the dot-separated path of every leaf of a configuration-shaped object, sorted. A plain object recurses and anything else - an array, a primitive, or
 * null - is a leaf, which is the reading of a leaf the reactivity diff takes, so a path listed here is a path a save's diff can report. The reactivity drift
 * tests share this one walker, so they cannot disagree about which leaves the configuration defines.
 * @param root - The object to walk. Defaults to DEFAULTS, the configuration's complete shape.
 * @returns The leaf paths, sorted.
 */
export function listConfigLeafPaths(root: object = DEFAULTS): readonly string[] {

  const paths: string[] = [];

  const walk = (value: unknown, prefix: string): void => {

    if(!isPlainObject(value)) {

      paths.push(prefix);

      return;
    }

    for(const [ key, child ] of Object.entries(value)) {

      walk(child, prefix + "." + key);
    }
  };

  for(const [ key, child ] of Object.entries(root)) {

    walk(child, key);
  }

  return paths.toSorted();
}
