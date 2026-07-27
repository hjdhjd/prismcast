/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * reader-during-writer.test.ts: Pins the file-store framework's atomicity contract under read-during-write contention. The framework's safety guarantee
 * (createFileStore docstring's atomic-writes bullet in src/config/persistence.ts): writes are atomic via temp+rename - rename() is atomic on POSIX and NTFS.
 * The structural consequence is that any concurrent reader either sees the old inode (pre-write) or the new inode (post-write), never partial bytes from a write
 * in flight.
 * Without this guarantee, a reader could observe a truncated JSON file mid-flush and produce a parse error or - worse - silently parse incomplete content.
 *
 * The cross-store-isolation suite covers the *result* guarantee for parallel writes to different stores: each store's bytes carry only its own data
 * after the dust settles. This suite covers the *timing* guarantee: while the writes are in flight, readers must see well-formed content at every moment.
 * Together they pin both axes of the framework's transactional contract.
 *
 * Why a burst-of-N rather than a single race: a single concurrent read-write pair will almost always race far enough apart that the read either fully precedes
 * or fully follows the write, leaving the atomic-rename branch untested in practice. A burst-of-N drives many overlapping pairs so the timing window is hit
 * statistically, while keeping the test deterministic in its success criteria - "every read parses to a well-formed shape" passes regardless of how the
 * specific schedule lands. The test does not assert WHICH shape (pre or post) any individual read sees because the framework explicitly does not order
 * concurrent reads against in-flight writes; the contract is about correctness, not visibility ordering.
 *
 * Why no controlled injection-point mutator for the read-during-write tests: a deterministic injection point would only be required if reproducing the race
 * were flaky. The burst approach proved deterministic-enough in practice (every read parses cleanly across many runs) without
 * requiring a production debug hook, so we ship this shape rather than introduce a test-only injection point. If a future flake emerges, the recourse is to
 * switch to a production-side debug hook (e.g., one in createFileStore that injects a configurable delay between writeFile and rename) rather than to flake-
 * tolerate the test - that is the "no bandaid" rule applied to test infrastructure as well as to production code.
 *
 * Per-store queue independence under contention: a timing-focused test could pin "concurrent writes to different stores complete independently
 * with consistent timing" - the *timing* invariant complementing cross-store-isolation's *result* invariant. Pinning this deterministically requires either
 * a production-side debug hook (an injectable delay between writeFile and rename so a slow store-A write provably overlaps a fast store-B write) or wall-clock
 * comparisons against a serial baseline. The latter approach was tried and proved too noisy at the sub-10ms range (concurrent and serial baselines fall
 * inside one another's jitter), and a wall-clock test that happens to pass on this hardware would be a flake on slower CI. The former requires a production
 * refactor for testability, which we avoid here. The structural invariant ("per-store queue, not a shared
 * queue") is already proven indirectly: the `let queue` declaration in createFileStore is a closure-scoped let inside each store's
 * factory call, so two stores cannot share one queue by construction. cross-store-isolation's concurrent-mutation test proves the result invariant. The timing
 * invariant is therefore intentionally not pinned by this suite; the architectural reading plus the existing cross-store coverage carry the weight a flaky timing test
 * would have.
 */
import { bootApp, createIntegrationContext, initializePersistence, pathInDataDir } from "../../helpers/integration.helpers.ts";
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { mutateChannels } from "../../../src/config/userChannels.ts";
import { readFile } from "node:fs/promises";

describe("file-store atomicity under read-during-write contention", () => {

  test("a burst of concurrent reads against a stream of mutations always parses to well-formed JSON; no reader observes a partial write", async () => {

    /* Drive a burst of mutateChannels calls (writers) in parallel with a burst of raw-file reads (readers) against the same channels.json. The framework
     * serializes the writers through its per-store queue, but the readers - which use fs.readFile directly, not the store's read() - are not coordinated with
     * the writers at all. They land at arbitrary moments during the writer cadence, exercising the atomic-rename contract end-to-end.
     *
     * Success criterion: every reader's content parses to a JSON object with the channels-file shape - a flat top-level layout where schemaVersion sits alongside
     * the channel entries (and migrationsApplied appears only once at least one migration has been applied, so a fresh store like this one omits it). A regression
     * that allowed a non-atomic write (e.g., writing directly to the main file instead of via temp+
     * rename) would surface here as occasional JSON.parse errors when the reader caught the file mid-flush.
     *
     * The test uses a substantial number of writers so the readers' wall-clock window genuinely overlaps several writes. Empirically, 25 writers + 50 readers
     * keeps the test under 500ms while reliably overlapping multiple flushes.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Seed an initial channel so the file exists on disk before the burst starts. Without this, the very first reads would see ENOENT before any writer has
    // landed - a different failure mode than the partial-write case under test.
    await mutateChannels((data) => { data.channels["seed-channel"] = { name: "Seed", url: "https://example.test/seed" }; });

    const channelsPath = pathInDataDir(ctx, "channels.json");
    const writerCount = 25;
    const readerCount = 50;

    // Writers: each adds a uniquely-keyed channel so the per-store mutator queue serializes them and every write touches the disk. The unique key prevents
    // collisions and makes the post-burst total verifiable.
    const writers = Array.from({ length: writerCount }, (_, i) => mutateChannels((data) => {

      data.channels["burst-" + String(i)] = { name: "Burst " + String(i), url: "https://example.test/burst-" + String(i) };
    }));

    // Readers: each does an independent readFile + JSON.parse of channels.json. Wrapped in an async function so the parse error (if any) surfaces with a
    // useful message including the offending content slice for triage.
    const readers = Array.from({ length: readerCount }, async (_, i): Promise<unknown> => {

      const content = await readFile(channelsPath, "utf-8");

      try {

        return JSON.parse(content);
      } catch(parseError) {

        const slice = content.slice(0, 100);
        const message = (parseError instanceof Error) ? parseError.message : String(parseError);

        throw new Error("Reader " + String(i) + " observed partial write: parse error '" + message + "' on content head: " + JSON.stringify(slice));
      }
    });

    // Await both batches together. Promise.all rejects on the first reader-side parse failure, which is the failure mode under test.
    const [ , readResults ] = await Promise.all([ Promise.all(writers), Promise.all(readers) ]);

    // Every read must yield a JSON object with the channels-file flat shape: channels are top-level keys alongside schemaVersion (migrationsApplied appears only
    // once a migration has run, so this fresh store omits it), not nested under a "channels" wrapper. A reader that observed a truncated write would have already
    // failed at JSON.parse above; this assertion catches the
    // (theoretically possible) case where a partial write happened to be valid JSON but had the wrong shape.
    for(const [ i, parsed ] of readResults.entries()) {

      assert.equal(typeof parsed, "object", "reader " + String(i) + " must observe an object, not a primitive");
      assert.notEqual(parsed, null, "reader " + String(i) + " must observe a non-null object");

      const obj = parsed as Record<string, unknown>;

      // Every read must include schemaVersion - the framework's beforeWrite stamps it on every persisted version, so its absence means a partial write or a
      // serialization regression.
      assert.equal(typeof obj["schemaVersion"], "number", "reader " + String(i) + " must observe a numeric schemaVersion at every snapshot");
    }

    // After the burst settles, every writer's channel must be present in the final file. The per-store queue's serialization guarantee is the contract: writes
    // never get lost under contention. Combined with the per-read shape assertion above, this pins both the in-flight invariant (no partial visibility) and
    // the post-burst invariant (no lost writes).
    const finalContent = await readFile(channelsPath, "utf-8");
    const finalParsed = JSON.parse(finalContent) as Record<string, unknown>;

    for(let i = 0; i < writerCount; i++) {

      assert.equal(typeof finalParsed["burst-" + String(i)], "object",
        "the final channels.json must include channel burst-" + String(i) + " - no writes lost under contention");
    }
  });

  test("readers driven through the production HTTP surface during a writer burst always receive a well-formed playlist response", async () => {

    /* The same atomicity invariant as the raw-read atomicity test above, but exercised through the production HTTP surface. The /playlist route reads the
     * merged channel state at request time; if a reader's request lands during a writer's atomic-rename window and the framework's contract is broken, the
     * reader could see partial data and produce a malformed playlist body. Since /playlist returns a 200 with an M3U body, "malformed" surfaces as either
     * an empty body or a body missing the #EXTM3U preamble.
     *
     * Where the raw-read atomicity test above exercised raw fs.readFile, this exercises the actual end-to-end pipeline (HTTP request, route handler,
     * channel-state read, body composition). It pins the invariant at the layer the user actually hits.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    const { urlFor } = await bootApp(ctx);

    // Seed a baseline channel so the playlist body is non-empty before the burst.
    await mutateChannels((data) => { data.channels["http-seed"] = { name: "HTTP Seed", url: "https://example.test/http-seed" }; });

    // Smaller than the raw-read atomicity test above because each reader here is a full HTTP round-trip rather than a raw fs.readFile, and the test still
    // needs to stay well under the suite's timeout.
    const writerCount = 15;
    const readerCount = 30;

    const writers = Array.from({ length: writerCount }, (_, i) => mutateChannels((data) => {

      data.channels["http-burst-" + String(i)] = { name: "HTTP Burst " + String(i), url: "https://example.test/http-burst-" + String(i) };
    }));

    const readers = Array.from({ length: readerCount }, async (): Promise<string> => {

      const response = await fetch(urlFor("/playlist"));

      assert.equal(response.status, 200, "playlist route must respond 200 even under writer contention");

      return response.text();
    });

    const [ , bodies ] = await Promise.all([ Promise.all(writers), Promise.all(readers) ]);

    // Every body must be a well-formed M3U playlist. Empty bodies, truncated bodies, and bodies missing the #EXTM3U preamble all indicate the channel state
    // was observed mid-write.
    for(const [ i, body ] of bodies.entries()) {

      assert.ok(body.length > 0, "reader " + String(i) + " must receive a non-empty playlist body");
      assert.match(body, /^#EXTM3U/, "reader " + String(i) + " must receive a playlist body starting with #EXTM3U - confirming a well-formed response under contention");
    }
  });

  test("the per-store mutator queue preserves submission order: a burst of mutations to one store applies in FIFO sequence", async () => {

    /* The per-store queue's structural contract: mutations submitted to one store run one at a time in FIFO order, regardless of how many were submitted in
     * parallel. The integration value over the framework's unit tests: this exercises the real production wrapper (mutateChannels), the real channels-store
     * registration, and the real beforeWrite + post-mutate hydration - layers a unit test of mutate() in isolation cannot reach.
     *
     * The test submits N mutations in order, each setting the same key to a sequence-numbered value. After all settle, the on-disk value must equal the
     * highest-numbered submission - the LAST writer wins because the queue ran them in submission order and each write overwrote the previous. A regression
     * that parallelized within a store would race them, and the on-disk value would be non-deterministic - this assertion catches that drift.
     *
     * This complements the raw-read atomicity test above (which proves no in-flight read sees a partial write) and the HTTP-surface atomicity test above
     * (which proves the same at the HTTP layer). Together the three tests pin: writes are atomic on disk (the raw-read and HTTP-surface atomicity tests
     * above), writes are serialized within a store (this test), and writes never produce partial results visible to concurrent readers (the raw-read and
     * HTTP-surface atomicity tests above). The cross-store independence invariant - which would deserve its own timing-focused test - is intentionally not
     * pinned here for the reasons documented in the file header's per-store-queue-independence note.
     */
    await using ctx = await createIntegrationContext();

    await initializePersistence(ctx);

    // Fewer than the raw-read atomicity test's writer count above because this test only needs enough submissions to prove FIFO ordering, not to force
    // overlapping flushes.
    const writeCount = 20;

    // Submit N mutations in tight succession without await. Each write sets the same key's name to a sequence-numbered value. The per-store queue must
    // serialize them; the final on-disk value is therefore the LAST-submitted value (writeCount - 1).
    const writes = Array.from({ length: writeCount }, (_, i) => mutateChannels((data) => {

      data.channels["queue-order-test"] = { name: "Order " + String(i), url: "https://example.test/order-" + String(i) };
    }));

    await Promise.all(writes);

    // Read the persisted channels.json. The queue-order-test entry's name must equal "Order " + (writeCount - 1) - the last submission. Any other value
    // indicates the queue ran writes out of order or in parallel.
    const persisted = JSON.parse(await readFile(pathInDataDir(ctx, "channels.json"), "utf-8")) as Record<string, unknown>;
    const entry = persisted["queue-order-test"];

    assert.equal(typeof entry, "object", "the queue-order-test entry must exist after the burst");
    assert.equal((entry as Record<string, unknown>)["name"], "Order " + String(writeCount - 1),
      "the per-store queue must apply writes in FIFO submission order - the final value is the last submission");
  });
});
