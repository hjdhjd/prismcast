/* Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
 *
 * page.helpers.test.ts: Unit tests for the general Page double. The double's whole value is that calls stay open until the test settles them and that every
 * call is recorded with the clock value it was issued at, so those two properties plus the per-member handler contract are what these tests pin.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { flushMicrotasks } from "./fn.helpers.ts";
import { makeFakePage } from "./page.helpers.ts";

describe("makeFakePage", () => {

  test("holds an evaluate open until the test settles it, and records the call", async () => {

    const fake = makeFakePage();

    let settled = "pending";

    const call = fake.page.evaluate(() => 1).then((value) => { settled = "resolved:" + String(value); });

    await flushMicrotasks(5);

    assert.equal(settled, "pending", "an unanswered evaluate stays outstanding");
    assert.equal(fake.evaluations.length, 1, "the call was recorded");

    fake.evaluations[0]?.resolve(42);

    await call;

    assert.equal(settled, "resolved:42", "the recorded handle answers the outstanding call");
  });

  test("rejects an evaluate through the recorded handle", async () => {

    const fake = makeFakePage();

    const call = fake.page.evaluate(() => 1);

    fake.evaluations[0]?.reject(new Error("context gone"));

    await assert.rejects(call, /context gone/, "the rejection reaches the caller");
  });

  test("answers through a per-call handler, with the call's index", async () => {

    // The handler form is what a test uses when every call takes the same canned answer: it settles the call as it arrives, so production code never blocks.
    const fake = makeFakePage({ onEvaluate: (evaluation, index) => { evaluation.resolve(index); } });

    assert.equal(await fake.page.evaluate(() => 1), 0, "first call answered with its index");
    assert.equal(await fake.page.evaluate(() => 1), 1, "second call answered with its index");
    assert.equal(fake.evaluations.length, 2, "both calls recorded in issue order");
  });

  test("records the clock value each call was issued at", async (t) => {

    // The timestamp is what lets a test pin when production code chose to make a call rather than only how many it made.
    t.mock.timers.enable({ apis: [ "setTimeout", "Date" ] });

    const fake = makeFakePage();

    void fake.page.evaluate(() => 1);

    t.mock.timers.tick(5000);

    void fake.page.evaluate(() => 1);

    assert.equal(fake.evaluations[0]?.at, 0, "first call stamped at the starting clock value");
    assert.equal(fake.evaluations[1]?.at, 5000, "second call stamped after the advance");
  });

  test("holds goto and waitForSelector open the same way", async () => {

    const fake = makeFakePage();

    const navigation = fake.page.goto("https://example.test/");
    const wait = fake.page.waitForSelector("video");

    assert.equal(fake.navigations.length, 1, "the navigation was recorded");
    assert.equal(fake.selectorWaits.length, 1, "the selector wait was recorded");

    fake.navigations[0]?.reject(new Error("navigation refused"));
    fake.selectorWaits[0]?.reject(new Error("no video"));

    await assert.rejects(navigation, /navigation refused/);
    await assert.rejects(wait, /no video/);
  });

  test("reports the configured url, frames, and browser pages, and follows setUrl and setClosed", async () => {

    const frame = { name: "child" };
    const other = { id: "other-page" };
    const fake = makeFakePage({ frames: [frame], pages: [other], url: "https://start.test/" });

    assert.equal(fake.page.url(), "https://start.test/", "the configured url is reported");
    assert.deepEqual(fake.page.frames(), [frame], "the configured frames are reported");
    assert.equal(fake.page.isClosed(), false, "a fresh double reports open");

    assert.deepEqual(await fake.page.browser().pages(), [other], "browser().pages() resolves with the configured list");

    fake.setUrl("https://moved.test/");
    fake.setClosed(true);

    assert.equal(fake.page.url(), "https://moved.test/", "setUrl takes effect");
    assert.equal(fake.page.isClosed(), true, "setClosed takes effect");
  });

  test("defaults frames and browser pages to empty", async () => {

    // Boundary: the defaults must be safe for code that counts pages before and after a navigation, which is the shape recovery code uses.
    const fake = makeFakePage();

    assert.deepEqual(fake.page.frames(), [], "no frames by default");
    assert.deepEqual(await fake.page.browser().pages(), [], "no pages by default");
  });
});
