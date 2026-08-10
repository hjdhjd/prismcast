/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * gridSearch.test.ts: Unit tests for the shared virtualized-guide search in gridSearch.ts. The module exports one function, so the describes split by behavior
 * surface rather than by export: how a search ends, how it narrows its range, what it promises about the rows a probe hands it, and what an in-range miss can
 * do with them.
 *
 * The probe is a plain callback, so none of this needs a page or a DOM - a scripted list of results and a record of the row indices asked for is the whole
 * harness. Those indices carry most of the weight here. The search reports its outcome but never its bounds, so where it probes next is the only evidence of
 * where it moved them: a window that narrows upward is a lower next index, one that narrows downward a higher one, and one that told the search nothing at all
 * is the same index over again.
 *
 * Two of the rows below are worth more than the boundary they name. The order-fidelity row fails the moment the search sorts or reorders anything, because its
 * fixture arrives deliberately out of name order and the direction it takes is the one the rows as given imply. The rows-identity row fails the moment the
 * search hands a miss hook a copy or a filtered subset instead of the probe's own array. Both encode the same contract from opposite sides: rows belong to the
 * probe, and a guide that files a channel under a name it does not display depends on the search leaving them exactly as they came.
 */
import { describe, test } from "node:test";
import type { GridProbeResult } from "./gridSearch.ts";
import assert from "node:assert/strict";
import { searchVirtualizedGrid } from "./gridSearch.ts";

// A row as the search sees it: a name it compares through nameOf, and nothing else it knows how to read.
interface TestRow {

  readonly name: string;
}

// US broadcast call sign shape, matching the exclusion a guide applies when a row's displayed name is not the name it sorts under.
const CALL_SIGN = /^[WK][A-Z]{2,3}$/i;

// A guide tall enough that each narrowing lands on a distinct, easily-read midpoint: 0-99 probes at 49, then 24 upward or 74 downward.
const TALL_GUIDE = 100;

// window builds one probe's worth of rows from names, in exactly the order given.
function window(...names: string[]): TestRow[] {

  return names.map((name) => ({ name }));
}

// nameOf reads the name the search compares a row by.
function nameOf(row: TestRow): string {

  return row.name;
}

// isCallSign marks the rows a guide cannot trust to sort where they appear.
function isCallSign(row: TestRow): boolean {

  return CALL_SIGN.test(row.name);
}

/* recordingProbe replays a scripted list of probe results and records the row index each probe was asked for. Once the script runs out it answers as a probe
 * that rendered nothing, which is the read that leaves the range alone - so a search that keeps going past the script spins on one index rather than wandering
 * somewhere the fixture never described.
 */
function recordingProbe<TFound>(results: GridProbeResult<TestRow, TFound>[]): {
  indices: number[];
  probe: (rowIndex: number) => Promise<GridProbeResult<TestRow, TFound>>;
} {

  const indices: number[] = [];

  return {

    indices,
    probe: async (rowIndex: number): Promise<GridProbeResult<TestRow, TFound>> => {

      indices.push(rowIndex);

      await Promise.resolve();

      return results[indices.length - 1] ?? { found: null, rows: null };
    }
  };
}

describe("virtualized grid search outcomes", () => {

  test("returns a hit as soon as a probe reports one, without probing again", async () => {

    const scripted = recordingProbe<string>([{ found: "espn", rows: window("alpha", "espn", "zulu") }]);

    const outcome = await searchVirtualizedGrid<TestRow, string>({

      maxIterations: 10,
      nameOf,
      probe: scripted.probe,
      targetName: "espn",
      totalRows: TALL_GUIDE
    });

    assert.deepEqual(outcome, { found: "espn", kind: "found" }, "a probe's hit is the search's outcome, payload and all");
    assert.deepEqual(scripted.indices, [49], "the search stops at the probe that found the target");
  });

  test("reports not found once the iteration cap is spent", async () => {

    // Every window sorts before the target, so the search keeps narrowing downward and never runs out of range - the cap is the only thing that ends it.
    const scripted = recordingProbe<string>([

      { found: null, rows: window("alpha") },
      { found: null, rows: window("bravo") },
      { found: null, rows: window("charlie") }
    ]);

    const outcome = await searchVirtualizedGrid<TestRow, string>({

      maxIterations: 2,
      nameOf,
      probe: scripted.probe,
      targetName: "zulu",
      totalRows: TALL_GUIDE
    });

    assert.deepEqual(outcome, { kind: "notFound" }, "a search that spends its cap reports the target missing rather than guessing");
    assert.equal(scripted.indices.length, 2, "the cap bounds the probes exactly, leaving the third scripted window unread");
  });

  test("reports not found once the range closes, without spending the remaining iterations", async () => {

    // A one-row guide gives the search a single position to try. Whichever way that window points, the range is empty on the next pass.
    const scripted = recordingProbe<string>([{ found: null, rows: window("alpha") }]);

    const outcome = await searchVirtualizedGrid<TestRow, string>({

      maxIterations: 5,
      nameOf,
      probe: scripted.probe,
      targetName: "zulu",
      totalRows: 1
    });

    assert.deepEqual(outcome, { kind: "notFound" }, "an exhausted range ends the search");
    assert.deepEqual(scripted.indices, [0], "the closed range ends the search immediately rather than burning the remaining iterations");
  });

  test("reports not found when the target sorts inside a window that has no miss policy", async () => {

    const scripted = recordingProbe<string>([{ found: null, rows: window("alpha", "zulu") }]);

    const outcome = await searchVirtualizedGrid<TestRow, string>({

      maxIterations: 10,
      nameOf,
      probe: scripted.probe,
      targetName: "mike",
      totalRows: TALL_GUIDE
    });

    assert.deepEqual(outcome, { kind: "notFound" }, "for a guide with nothing to recover, an in-range miss means the channel is simply absent");
    assert.deepEqual(scripted.indices, [49], "the search stops there rather than re-probing a window it has already read");
  });
});

describe("virtualized grid range narrowing", () => {

  test("narrows upward when the target sorts before everything rendered", async () => {

    const scripted = recordingProbe<string>([{ found: null, rows: window("mike", "zulu") }]);

    await searchVirtualizedGrid<TestRow, string>({

      maxIterations: 3,
      nameOf,
      probe: scripted.probe,
      targetName: "alpha",
      totalRows: TALL_GUIDE
    });

    assert.deepEqual(scripted.indices.slice(0, 2), [ 49, 24 ], "the next probe lands in the half above the window");
  });

  test("narrows downward when the target sorts after everything rendered", async () => {

    const scripted = recordingProbe<string>([{ found: null, rows: window("alpha", "bravo") }]);

    await searchVirtualizedGrid<TestRow, string>({

      maxIterations: 3,
      nameOf,
      probe: scripted.probe,
      targetName: "zulu",
      totalRows: TALL_GUIDE
    });

    assert.deepEqual(scripted.indices.slice(0, 2), [ 49, 74 ], "the next probe lands in the half below the window");
  });

  test("leaves the range alone when a probe rendered nothing", async () => {

    // A virtualizer caught mid-recycle renders no rows. That is not evidence about where the target sits, so the range must not move on it - the search simply
    // asks the same position again.
    const scripted = recordingProbe<string>([

      { found: null, rows: null },
      { found: null, rows: [] }
    ]);

    await searchVirtualizedGrid<TestRow, string>({

      maxIterations: 3,
      nameOf,
      probe: scripted.probe,
      targetName: "mike",
      totalRows: TALL_GUIDE
    });

    assert.deepEqual(scripted.indices, [ 49, 49, 49 ], "an empty read and a null read both leave the bounds where they were");
  });

  test("treats a probe reporting both an empty window and a hit as the empty read it is", async () => {

    /* Emptiness is settled before the hit is read. A probe has to answer both questions in one pass, and a hit on a window nobody saw rendered is the one
     * combination where those answers disagree - the search believes the window, not the hit, and asks again.
     */
    const scripted = recordingProbe<string>([

      { found: "wabc", rows: [] },
      { found: "wabc", rows: window("wabc") }
    ]);

    const outcome = await searchVirtualizedGrid<TestRow, string>({

      maxIterations: 5,
      nameOf,
      probe: scripted.probe,
      targetName: "abc",
      totalRows: TALL_GUIDE
    });

    assert.deepEqual(outcome, { found: "wabc", kind: "found" }, "the hit on the window that did render is the one the search takes");
    assert.deepEqual(scripted.indices, [ 49, 49 ], "the empty window neither returned its hit nor moved the bounds");
  });

  test("moves down when a window holds no rows the caller will anchor on", async () => {

    // The target sorts before both rows, so a search anchoring on them would go up. Excluding them leaves no direction to read at all, and the fallback is to
    // move down and hope the next window renders something comparable.
    const scripted = recordingProbe<string>([{ found: null, rows: window("wabc", "kxyz") }]);

    await searchVirtualizedGrid<TestRow, string>({

      isDirectionAnchor: (row): boolean => !isCallSign(row),
      maxIterations: 3,
      nameOf,
      probe: scripted.probe,
      targetName: "alpha",
      totalRows: TALL_GUIDE
    });

    assert.deepEqual(scripted.indices.slice(0, 2), [ 49, 74 ], "an unanchorable window sends the search down rather than up");
  });
});

describe("virtualized grid row contract", () => {

  test("reads direction from the rows as the probe returned them, never from a sorted copy", async () => {

    /* The fixture arrives deliberately out of name order. Read as given, the target sorts before the first row and the search goes up. Sorted first, the target
     * would fall between the two extremes and the search would stop on an in-range miss. The next probe index is what tells those two apart.
     */
    const scripted = recordingProbe<string>([{ found: null, rows: window("zulu", "alpha") }]);

    await searchVirtualizedGrid<TestRow, string>({

      maxIterations: 3,
      nameOf,
      probe: scripted.probe,
      targetName: "mike",
      totalRows: TALL_GUIDE
    });

    assert.deepEqual(scripted.indices.slice(0, 2), [ 49, 24 ], "the first and last rows are the ones the probe put first and last");
  });

  test("compares only the anchored rows while handing the miss hook every row", async () => {

    // Anchoring on the call sign would put the target inside the window and stop the search. Anchoring on the one trustworthy row puts it after, and the search
    // moves down.
    const scripted = recordingProbe<string>([{ found: null, rows: window("alpha", "wabc") }]);

    await searchVirtualizedGrid<TestRow, string>({

      isDirectionAnchor: (row): boolean => !isCallSign(row),
      maxIterations: 3,
      nameOf,
      probe: scripted.probe,
      targetName: "mike",
      totalRows: TALL_GUIDE
    });

    assert.deepEqual(scripted.indices.slice(0, 2), [ 49, 74 ], "the excluded row takes no part in the direction decision");
  });

  test("hands the miss hook the probe's own array, unfiltered and in order", async () => {

    /* A hook that infers a channel from where it sits among its neighbors needs the whole window in the guide's order - the filtered anchors would be a
     * different list with different positions, and a copy would break any identity the hook's caller relies on.
     */
    const rendered = window("alpha", "wabc", "zulu");
    const scripted = recordingProbe<string>([{ found: null, rows: rendered }]);

    let captured: TestRow[] | undefined;

    await searchVirtualizedGrid<TestRow, string>({

      isDirectionAnchor: (row): boolean => !isCallSign(row),
      maxIterations: 3,
      nameOf,
      onInRangeMiss: async (rows): Promise<null> => {

        captured = rows;

        await Promise.resolve();

        return null;
      },
      probe: scripted.probe,
      targetName: "mike",
      totalRows: TALL_GUIDE
    });

    assert.equal(captured, rendered, "the hook receives the very array the probe returned rather than a copy of it");
    assert.deepEqual(captured.map(nameOf), [ "alpha", "wabc", "zulu" ], "the excluded row is still present, in the position the probe gave it");
  });
});

describe("virtualized grid in-range miss", () => {

  test("ends the search as resolved when the hook answers the whole request itself", async () => {

    const scripted = recordingProbe<string>([{ found: null, rows: window("alpha", "zulu") }]);

    const outcome = await searchVirtualizedGrid<TestRow, string, { tuned: string }>({

      maxIterations: 5,
      nameOf,
      onInRangeMiss: async (): Promise<{ resolved: { tuned: string } }> => {

        await Promise.resolve();

        return { resolved: { tuned: "wabc" } };
      },
      probe: scripted.probe,
      targetName: "mike",
      totalRows: TALL_GUIDE
    });

    assert.deepEqual(outcome, { kind: "resolved", result: { tuned: "wabc" } }, "the hook's result is carried out of the search untouched");
    assert.deepEqual(scripted.indices, [49], "a resolved request ends the search where it stood");
  });

  test("ends the search as found when the hook identifies the row after all", async () => {

    const scripted = recordingProbe<string>([{ found: null, rows: window("alpha", "zulu") }]);

    const outcome = await searchVirtualizedGrid<TestRow, string, { tuned: string }>({

      maxIterations: 5,
      nameOf,
      onInRangeMiss: async (): Promise<{ found: string }> => {

        await Promise.resolve();

        return { found: "wabc" };
      },
      probe: scripted.probe,
      targetName: "mike",
      totalRows: TALL_GUIDE
    });

    assert.deepEqual(outcome, { found: "wabc", kind: "found" }, "a hook that recovers a name reports it as the hit the probe could not make");
    assert.deepEqual(scripted.indices, [49], "the recovered hit ends the search where it stood");
  });

  test("reports not found when the hook has nothing to offer", async () => {

    const scripted = recordingProbe<string>([{ found: null, rows: window("alpha", "zulu") }]);

    const outcome = await searchVirtualizedGrid<TestRow, string, { tuned: string }>({

      maxIterations: 5,
      nameOf,
      onInRangeMiss: async (): Promise<null> => {

        await Promise.resolve();

        return null;
      },
      probe: scripted.probe,
      targetName: "mike",
      totalRows: TALL_GUIDE
    });

    assert.deepEqual(outcome, { kind: "notFound" }, "a hook declining to recover leaves the target missing");
    assert.deepEqual(scripted.indices, [49], "declining ends the search rather than sending it back around the range");
  });
});
