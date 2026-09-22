import { test } from "node:test";
import assert from "node:assert/strict";

import { memoryStorage, OfflineQueue, type Operation, type Outcome } from "../src/index.ts";

const at = (iso: string) => () => new Date(iso);

/** Let everything pending settle, so "what is on the wire" is a stable answer. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

/** The app's own notion of what a change touches: one note. */
const entityOf = (operation: Operation) =>
  (operation.payload as { note?: string }).note ?? operation.kind;

function queue(send: (operation: Operation) => Promise<Outcome>, concurrency?: number) {
  return new OfflineQueue({
    storage: memoryStorage(),
    send,
    entityOf,
    now: at("2026-08-16T10:00:00Z"),
    random: () => 0.5,
    ...(concurrency === undefined ? {} : { concurrency }),
  });
}

/** A queue whose transport holds every attempt open until the gate is opened. */
function gated(concurrency: number) {
  const started: string[] = [];
  let open: () => void = () => {};
  const gate = new Promise<void>((resolve) => {
    open = resolve;
  });
  const q = queue(async (operation) => {
    started.push(entityOf(operation));
    await gate;
    return { result: "done" };
  }, concurrency);
  return { q, started, open: () => open() };
}

// A queue that reorders an edit and a delete applies them backwards.
test("changes to one entity are sent in order and stop at the first retry", async () => {
  const seen: string[] = [];
  const q = queue(async (operation) => {
    seen.push(operation.kind);
    return { result: "retry", reason: "offline" };
  });
  await q.enqueue("title", { note: "42", title: "Dinner" });
  await q.enqueue("remove", { note: "42" });

  const report = await q.flush();

  assert.deepEqual(seen, ["title"], "the delete does not overtake the edit it follows");
  assert.equal(report.retrying, 1);
});

test("an entity that is retrying does not hold up an unrelated one", async () => {
  const seen: string[] = [];
  const q = queue(async (operation) => {
    const entity = entityOf(operation);
    seen.push(entity);
    return entity === "1" ? { result: "retry", reason: "offline" } : { result: "done" };
  });
  await q.enqueue("edit", { note: "1", text: "first" });
  await q.enqueue("edit", { note: "1", text: "second" });
  await q.enqueue("edit", { note: "2", text: "unrelated" });

  const report = await q.flush();

  assert.equal(report.sent, 1, "the unrelated note went out");
  assert.equal(report.retrying, 1);
  assert.deepEqual([...seen].sort(), ["1", "2"], "the second change to note 1 waited");
});

// Parking exists so that one refusal stops one change, not the rest of the lane.
test("a parked change does not stop the rest of its entity", async () => {
  const seen: string[] = [];
  const q = queue(async (operation) => {
    seen.push(operation.kind);
    return operation.kind === "title"
      ? { result: "rejected", reason: "title is required" }
      : { result: "done" };
  });
  await q.enqueue("title", { note: "42", title: "" });
  await q.enqueue("star", { note: "42" });

  const report = await q.flush();

  assert.deepEqual(seen, ["title", "star"], "the lane carries on past the parked change");
  assert.equal(report.rejected, 1);
  assert.equal(report.sent, 1);
});

test("unrelated entities go out together, up to the limit", async () => {
  const lane = gated(2);
  for (const note of ["1", "2", "3", "4"]) await lane.q.enqueue("edit", { note });

  const flushing = lane.q.flush();
  await settle();
  assert.equal(lane.started.length, 2, "two entities are on the wire, not four");

  lane.open();
  const report = await flushing;

  assert.equal(report.sent, 4, "the rest went out as the first two finished");
  assert.deepEqual([...lane.started].sort(), ["1", "2", "3", "4"]);
});

// A phone on a weak signal, or a server that dislikes parallel writes.
test("a limit of one sends entities one at a time", async () => {
  const lane = gated(1);
  for (const note of ["1", "2", "3"]) await lane.q.enqueue("edit", { note });

  const flushing = lane.q.flush();
  await settle();
  assert.equal(lane.started.length, 1);

  lane.open();
  const report = await flushing;
  assert.equal(report.sent, 3);
});

// Without an entity of its own, a change is ordered against its kind — which is
// what the queue did before entities existed.
test("the kind is the entity when the app names none", async () => {
  const seen: string[] = [];
  const q = new OfflineQueue({
    storage: memoryStorage(),
    send: async (operation) => {
      seen.push(operation.kind);
      return operation.kind === "note"
        ? { result: "retry", reason: "offline" }
        : { result: "done" };
    },
    now: at("2026-08-16T10:00:00Z"),
    random: () => 0.5,
  });
  await q.enqueue("note", { text: "first" });
  await q.enqueue("note", { text: "second" });
  await q.enqueue("photo", { name: "beach.jpg" });

  await q.flush();

  assert.deepEqual([...seen].sort(), ["note", "photo"], "the second note waits, the photo does not");
});
