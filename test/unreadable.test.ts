/**
 * A store that could not be read is never written over.
 *
 * A read that failed is not knowledge that the device holds nothing. Taking it
 * for an empty queue is the data-loss path: one transient failure, one enqueue,
 * and every change that survived the night is written over. So the failure
 * travels, the queue stays unloaded, and `enqueue`, `recover` and `flush` read
 * again rather than writing over a queue nobody managed to see.
 *
 * One read answers every caller waiting on it, because a second read settling
 * on top of the first loses whatever was enqueued in between.
 */

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  keyValueStorage,
  memoryStorage,
  mmkvStorage,
  OfflineQueue,
  type Operation,
  type Outcome,
  type Storage,
} from "../src/index.ts";

const at = (iso: string) => () => new Date(iso);

const unavailable = (): never => {
  throw new Error("storage unavailable");
};

/** A store whose first read fails and whose later reads work. */
function throwsOnceOnRead(inner: Storage): Storage {
  let thrown = false;
  return {
    load: async () => {
      if (thrown) return inner.load();
      thrown = true;
      return unavailable();
    },
    save: (operations) => inner.save(operations),
  };
}

function queue(storage: Storage, send: (operation: Operation) => Promise<Outcome>) {
  return new OfflineQueue({
    storage,
    send,
    now: at("2026-08-16T10:00:00Z"),
    random: () => 0.5,
  });
}

const offline = async (): Promise<Outcome> => ({ result: "retry", reason: "offline" });
const accepted = async (): Promise<Outcome> => ({ result: "done" });

/** A row the process died on, with the lease it held. */
const interruptedCharge: Operation = {
  id: "op-1",
  kind: "charge",
  payload: { amount: 100 },
  idempotencyKey: "key-abc",
  createdAt: "2026-08-16T09:00:00.000Z",
  attempts: 1,
  state: "in-flight",
  startedAt: "2026-08-16T09:50:00.000Z",
};

// The adapters only read an empty queue out of a value they could read: a
// store that throws is not one of those.
test("AsyncStorage: a store that cannot be read says so", async () => {
  const storage = keyValueStorage({ getItem: unavailable, setItem: async () => {} });
  await assert.rejects(() => storage.load(), /storage unavailable/);
});

test("MMKV: a store that cannot be read says so", async () => {
  const storage = mmkvStorage({ getString: unavailable, set: () => {} });
  await assert.rejects(() => storage.load(), /storage unavailable/);
});

test("enqueueing over a failed read does not overwrite the stored queue", async () => {
  const cells = memoryStorage();
  const overnight = queue(cells, offline);
  await overnight.enqueue("note", { text: "survived the night" });

  const next = queue(throwsOnceOnRead(cells), accepted);
  await assert.rejects(() => next.enqueue("note", { text: "fresh" }), /storage unavailable/);
  assert.deepEqual(
    (await cells.load()).map((operation) => operation.payload),
    [{ text: "survived the night" }],
    "the queue nobody could read was not written over",
  );

  // It stayed unloaded, so the next call reads again — and adds to what it
  // found rather than to nothing.
  await next.enqueue("note", { text: "fresh" });
  assert.equal((await cells.load()).length, 2);
  assert.equal(next.outstanding.length, 2);
});

test("a flush sends nothing from a queue it could not read", async () => {
  const cells = memoryStorage();
  const first = queue(cells, offline);
  await first.enqueue("note", { text: "written on the train" });

  const sent: Operation[] = [];
  const q = queue(throwsOnceOnRead(cells), async (operation) => {
    sent.push(operation);
    return { result: "done" };
  });

  await assert.rejects(() => q.flush(), /storage unavailable/);
  assert.equal(sent.length, 0, "nothing was attempted");
  assert.equal((await cells.load()).length, 1, "and nothing was written");

  const report = await q.flush();
  assert.equal(report.sent, 1, "the next flush reads again and sends what was there");
});

test("recovery over a failed read leaves the interrupted row alone", async () => {
  const cells = memoryStorage([interruptedCharge]);
  const q = queue(throwsOnceOnRead(cells), accepted);

  await assert.rejects(() => q.recover(), /storage unavailable/);
  assert.equal((await cells.load())[0]?.state, "in-flight", "nothing was reclaimed blind");

  await q.recover();
  assert.equal(q.outstanding[0]?.state, "pending", "the second read reclaims it");
  assert.equal((await cells.load())[0]?.state, "pending");
});

// Two callers on one unloaded queue is a screen enqueueing while a reconnect
// flushes. They wait on one read, so one failure is one failure.
test("callers waiting on the same failed read all hear about it", async () => {
  const cells = memoryStorage();
  const overnight = queue(cells, offline);
  await overnight.enqueue("note", { text: "survived the night" });

  const q = queue(throwsOnceOnRead(cells), accepted);
  const outcomes = await Promise.allSettled([q.enqueue("note", { text: "fresh" }), q.flush()]);

  assert.deepEqual(
    outcomes.map((outcome) => outcome.status),
    ["rejected", "rejected"],
    "neither of them read an empty queue out of the failure",
  );
  assert.equal((await cells.load()).length, 1, "and nothing was written");

  await q.enqueue("note", { text: "fresh" });
  assert.equal((await cells.load()).length, 2, "the next caller reads again");
});

// A second read landing on top of the first is the same loss by another route:
// the row enqueued in between is not in the snapshot the later read returns.
test("two changes enqueued before the first read lands are both kept", async () => {
  const cells = memoryStorage();
  const overnight = queue(cells, offline);
  await overnight.enqueue("note", { text: "survived the night" });

  const q = queue(cells, accepted);
  const [first, second] = await Promise.all([
    q.enqueue("note", { text: "one" }),
    q.enqueue("note", { text: "two" }),
  ]);

  assert.notEqual(first.id, second.id);
  assert.equal(q.operations.length, 3);
  assert.equal((await cells.load()).length, 3, "neither change was read over by the other");
});

// A flush started from a connectivity event has no caller to throw to, and a
// read failure must still not be silent.
test("a read that failed during a flush on reconnect is reported", async () => {
  const cells = memoryStorage();
  const first = queue(cells, offline);
  await first.enqueue("note", { text: "survived the night" });

  const errors: unknown[] = [];
  const q = queue(throwsOnceOnRead(cells), accepted);
  let announce: ((online: boolean) => void) | undefined;
  const stop = q.start(
    (listener) => {
      announce = listener;
      return () => {
        announce = undefined;
      };
    },
    (error) => errors.push(error),
  );

  announce?.(true);
  await new Promise((resolve) => setTimeout(resolve, 0));

  assert.equal(errors.length, 1);
  assert.match((errors[0] as Error).message, /storage unavailable/);
  assert.equal((await cells.load()).length, 1, "the stored queue is untouched");

  announce?.(true);
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal((await cells.load())[0]?.state, "done", "the next event reads and flushes");
  stop();
});
