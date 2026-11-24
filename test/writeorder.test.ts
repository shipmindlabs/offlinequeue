/**
 * One write at a time.
 *
 * A flush runs several lanes at once and every one of them saves. Two stores
 * overlapping would let an older snapshot land last and undo an outcome that
 * was already recorded, so the writes queue behind each other and each one
 * takes the queue as it is when its turn comes.
 *
 * The other half of that chain is a write that failed. It is not swallowed —
 * losing a change silently is the failure this library exists to prevent — but
 * it is not allowed to hold the writes behind it either: the next save carries
 * what the lost one was going to say.
 */

import { test } from "node:test";
import assert from "node:assert/strict";

import { OfflineQueue, type Operation, type Outcome, type Storage } from "../src/index.ts";

const at = (iso: string) => () => new Date(iso);

/** Let everything pending settle, so "what is on the wire" is a stable answer. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

/** The app's own notion of what a change touches: one note. */
const entityOf = (operation: Operation) =>
  (operation.payload as { note?: string }).note ?? operation.kind;

/**
 * A store that takes a turn of the event loop to answer, which is all an
 * overlapping write needs in order to land out of order. It records how its
 * writes overlapped and what each of them was told.
 */
function slowDisk(options: { failOnWrite?: number } = {}) {
  let held: Operation[] = [];
  const snapshots: string[][] = [];
  let inFlight = 0;
  let peak = 0;
  let writes = 0;
  const storage: Storage = {
    load: async () => [...held],
    save: async (operations) => {
      writes++;
      const shape = operations.map((operation) => `${entityOf(operation)}:${operation.state}`);
      const taking = [...operations];
      inFlight++;
      peak = Math.max(peak, inFlight);
      await settle();
      inFlight--;
      if (writes === options.failOnWrite) throw new Error("disk full");
      held = taking;
      snapshots.push(shape);
    },
  };
  return {
    storage,
    snapshots,
    rows: () => held,
    get peak() {
      return peak;
    },
  };
}

/** A transport that holds every attempt open until the gate is opened. */
function gate() {
  let open: () => void = () => {};
  const held = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { held, open: () => open() };
}

function queue(
  storage: Storage,
  send: (operation: Operation) => Promise<Outcome>,
  concurrency = 4,
) {
  return new OfflineQueue({
    storage,
    send,
    entityOf,
    concurrency,
    now: at("2026-08-16T10:00:00Z"),
    random: () => 0.5,
  });
}

test("lanes on the wire together never ask the store to save two things at once", async () => {
  const device = slowDisk();
  const q = queue(device.storage, async () => ({ result: "done" }));
  for (const note of ["1", "2", "3", "4"]) await q.enqueue("edit", { note });

  const report = await q.flush();

  assert.equal(report.sent, 4);
  assert.equal(device.peak, 1, "an adapter is only ever asked for one write");
  assert.deepEqual(
    device.snapshots.at(-1),
    ["1:done", "2:done", "3:done", "4:done"],
    "no lane's outcome was undone by an older snapshot landing last",
  );
  assert.deepEqual(device.rows(), [...q.operations], "the disk ends where the queue does");
});

// A change made while the flush was out on the wire is not in the snapshot the
// flush started with, and the write that ends the flush must not drop it.
test("a change enqueued during a flush survives the flush's own write", async () => {
  const device = slowDisk();
  const door = gate();
  const q = queue(device.storage, async () => {
    await door.held;
    return { result: "done" };
  });
  await q.enqueue("edit", { note: "1" });

  const flushing = q.flush();
  await settle();
  await q.enqueue("edit", { note: "2" });
  door.open();
  const report = await flushing;

  assert.equal(report.sent, 1);
  assert.equal(report.remaining, true, "the change made during the flush is still waiting");
  assert.deepEqual(device.snapshots.at(-1), ["1:done", "2:pending"]);
  assert.equal(device.rows().length, 2, "two rows on the device, not one");
});

// The write chain recovers from a rejection: holding every later write behind
// one full disk would lose everything the queue did after it.
test("a write that failed does not hold the next one behind it", async () => {
  const device = slowDisk({ failOnWrite: 2 });
  const q = queue(device.storage, async () => ({ result: "done" }));

  await q.enqueue("edit", { note: "1" });
  await assert.rejects(() => q.enqueue("edit", { note: "2" }), /disk full/);
  await q.enqueue("edit", { note: "3" });

  assert.deepEqual(
    device.snapshots.at(-1),
    ["1:pending", "2:pending", "3:pending"],
    "the next write carries what the lost one was going to say",
  );
  assert.equal(q.outstanding.length, 3);
});

// The row is stored in flight before the request leaves, so a store that could
// not take it leaves nothing on the device to recognise a repeat by.
test("a flush that cannot record an attempt does not make it", async () => {
  const device = slowDisk({ failOnWrite: 2 });
  const sent: Operation[] = [];
  const q = queue(device.storage, async (operation) => {
    sent.push(operation);
    return { result: "done" };
  });
  await q.enqueue("edit", { note: "1" });

  await assert.rejects(() => q.flush(), /disk full/);

  assert.deepEqual(sent, [], "a request nobody could write down is not on the wire");
  assert.equal(device.rows()[0]?.state, "pending", "the disk holds the row as it was");
  assert.equal(device.rows()[0]?.startedAt, undefined, "and no lease over it");
});

// A misconfigured limit is a smaller problem than a queue that never flushes.
test("a limit below one still sends, one entity at a time", async () => {
  const device = slowDisk();
  let inFlight = 0;
  let peak = 0;
  const q = queue(
    device.storage,
    async () => {
      inFlight++;
      peak = Math.max(peak, inFlight);
      await settle();
      inFlight--;
      return { result: "done" };
    },
    0,
  );
  for (const note of ["1", "2"]) await q.enqueue("edit", { note });

  const report = await q.flush();

  assert.equal(report.sent, 2, "a limit of zero is not a queue that stops");
  assert.equal(peak, 1, "it falls back to one entity at a time");
});
