/**
 * A rejection the user fixed.
 *
 * `retry(id)` sends the same request again, which is right for a change that
 * only ran out of network and wrong for one the server refused on its content:
 * an idempotency key names one exact request, and a server that remembers keys
 * answers the old one. So a new payload travels under a new key.
 *
 * An exhausted change is the one that cannot be amended. Its attempts ran out
 * without an answer, so nobody knows whether the server applied it, and a new
 * key is exactly how it would be applied a second time.
 */

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  memoryStorage,
  OfflineQueue,
  type Operation,
  type Outcome,
  type Storage,
} from "../src/index.ts";

const at = (iso: string) => () => new Date(iso);

/**
 * A server that remembers every key it has answered and replays the answer —
 * which is what makes a key name one request rather than one row.
 */
function server() {
  const answers = new Map<string, Outcome>();
  const received: string[] = [];
  const applied: unknown[] = [];
  return {
    received,
    applied,
    send: async (operation: Operation): Promise<Outcome> => {
      received.push(operation.idempotencyKey);
      const remembered = answers.get(operation.idempotencyKey);
      if (remembered) return remembered;
      const title = (operation.payload as { title?: string }).title;
      const answer: Outcome = title
        ? { result: "done" }
        : { result: "rejected", reason: "title is required" };
      answers.set(operation.idempotencyKey, answer);
      if (answer.result === "done") applied.push(operation.payload);
      return answer;
    },
  };
}

function queue(
  send: (operation: Operation) => Promise<Outcome>,
  storage: Storage = memoryStorage(),
) {
  let keys = 0;
  return new OfflineQueue({
    storage,
    send,
    now: at("2026-08-16T10:00:00Z"),
    random: () => 0.5,
    newKey: () => `key-${++keys}`,
  });
}

/** A row that gave up without an answer, as a previous run left it. */
const exhaustedCharge: Operation = {
  id: "op-1",
  kind: "charge",
  payload: { amount: 100 },
  idempotencyKey: "key-abc",
  createdAt: "2026-08-16T09:00:00.000Z",
  attempts: 8,
  state: "failed",
  parkedReason: "exhausted",
  lastError: "offline",
};

/** A row the server refused, as a previous run left it. */
const rejectedNote: Operation = {
  id: "op-2",
  kind: "note",
  payload: { title: "" },
  idempotencyKey: "key-note",
  createdAt: "2026-08-16T09:00:00.000Z",
  attempts: 1,
  state: "failed",
  parkedReason: "rejected",
  lastError: "title is required",
};

/** A store whose first read fails and whose later reads work. */
function throwsOnceOnRead(inner: Storage): Storage {
  let thrown = false;
  return {
    load: async () => {
      if (thrown) return inner.load();
      thrown = true;
      throw new Error("storage unavailable");
    },
    save: (operations) => inner.save(operations),
  };
}

/** The same store, until it is sealed and refuses every further write. */
function sealable(inner: Storage) {
  let sealed = false;
  const storage: Storage = {
    load: () => inner.load(),
    save: async (operations) => {
      if (sealed) throw new Error("disk full");
      await inner.save(operations);
    },
  };
  return {
    storage,
    seal: () => {
      sealed = true;
    },
  };
}

test("an amended rejection is pending again, with the new payload and a new key", async () => {
  const api = server();
  const q = queue(api.send);
  const note = await q.enqueue("note", { title: "" });
  await q.flush();
  assert.equal(q.failed[0]?.parkedReason, "rejected");

  const amended = await q.amend(note.id, { title: "Dinner" });

  assert.equal(amended.id, note.id, "the same row, not a second one");
  assert.deepEqual(amended.payload, { title: "Dinner" });
  assert.equal(amended.state, "pending");
  assert.equal(amended.attempts, 0, "the limit applies to the new run");
  assert.equal(amended.parkedReason, undefined);
  assert.equal(amended.lastError, undefined, "the error described a payload that is gone");
  assert.equal(amended.nextAttemptAt, undefined);
  assert.notEqual(amended.idempotencyKey, note.idempotencyKey, "a new request, a new key");
  assert.equal(q.failed.length, 0);
  assert.equal(q.operations.length, 1);
});

// The whole reason the key changes: under the old one the fix never lands.
test("the new key is what makes the fix land", async () => {
  const api = server();
  const q = queue(api.send);
  const note = await q.enqueue("note", { title: "" });
  await q.flush();

  await q.retry(note.id);
  await q.flush();
  assert.equal(q.failed.length, 1, "the same request, answered from what the server remembers");
  assert.deepEqual(api.applied, []);

  await q.amend(note.id, { title: "Dinner" });
  const report = await q.flush();

  assert.equal(report.sent, 1);
  assert.deepEqual(api.applied, [{ title: "Dinner" }]);
  assert.deepEqual(
    api.received,
    ["key-1", "key-1", "key-2"],
    "two requests under the refused key, one under the amended one",
  );
});

test("an exhausted change is refused, because the server may have applied it", async () => {
  const cells = memoryStorage([exhaustedCharge]);
  const q = queue(async () => ({ result: "done" }), cells);
  await q.load();

  await assert.rejects(() => q.amend("op-1", { amount: 250 }), /exhausted/);

  assert.equal(q.failed[0]?.parkedReason, "exhausted", "it stayed where it was");
  assert.deepEqual(q.failed[0]?.payload, { amount: 100 }, "the payload the server may hold");
  assert.equal(q.failed[0]?.idempotencyKey, "key-abc", "and the key it would be recognised by");
  assert.deepEqual((await cells.load())[0]?.payload, { amount: 100 }, "nothing was written");
});

test("only a parked change can be amended", async () => {
  const api = server();
  const q = queue(api.send);
  const note = await q.enqueue("note", { title: "Dinner" });

  await assert.rejects(() => q.amend(note.id, { title: "Lunch" }), /not a parked change/);
  await q.flush();
  await assert.rejects(() => q.amend(note.id, { title: "Lunch" }), /not a parked change/);
  await assert.rejects(() => q.amend("op-404", { title: "Lunch" }), /not a parked change/);

  assert.deepEqual(q.operations[0]?.payload, { title: "Dinner" }, "what was sent is what was sent");
  assert.deepEqual(api.applied, [{ title: "Dinner" }]);
});

test("an amendment is on the device before it resolves", async () => {
  const cells = memoryStorage();
  const api = server();
  const q = queue(api.send, cells);
  const note = await q.enqueue("note", { title: "" });
  await q.flush();

  const amended = await q.amend(note.id, { title: "Dinner" });

  const stored = (await cells.load())[0];
  assert.deepEqual(stored?.payload, { title: "Dinner" });
  assert.equal(stored?.state, "pending");
  assert.equal(stored?.attempts, 0);
  assert.equal(stored?.parkedReason, undefined, "no trace of the parking survived the write");
  assert.equal(stored?.lastError, undefined);
  assert.equal(stored?.idempotencyKey, amended.idempotencyKey);
});

// The screen that enqueues on mount must still find one row, not two.
test("the amended change answers to its new key", async () => {
  const api = server();
  const q = queue(api.send);
  const note = await q.enqueue("note", { title: "" });
  await q.flush();
  const amended = await q.amend(note.id, { title: "Dinner" });

  const again = await q.enqueue("note", { title: "Dinner" }, amended.idempotencyKey);

  assert.equal(again.id, amended.id);
  assert.equal(q.operations.length, 1);
});

// A fix that is itself refused is still a rejection, so the way out is the same
// one — and each payload the server has not seen travels under its own key.
test("an amendment the server also refuses can be amended again", async () => {
  const api = server();
  const q = queue(api.send);
  const note = await q.enqueue("note", { title: "" });
  await q.flush();

  await q.amend(note.id, { title: "" });
  await q.flush();
  assert.equal(q.failed[0]?.parkedReason, "rejected", "refused on its content again");
  assert.equal(q.failed[0]?.attempts, 1, "one attempt, under the second key");

  const fixed = await q.amend(note.id, { title: "Dinner" });
  const report = await q.flush();

  assert.equal(report.sent, 1);
  assert.equal(fixed.idempotencyKey, "key-3");
  assert.deepEqual(api.received, ["key-1", "key-2", "key-3"], "three payloads, three keys");
  assert.deepEqual(api.applied, [{ title: "Dinner" }], "and one of them applied");
  assert.equal(q.operations.length, 1, "still one row");
});

// What cannot be amended is an exhausted row, not a row that was once
// exhausted: `retry` under the original key is how it gets an answer, and an
// answer is what makes the fix safe to send under a new one.
test("an exhausted change the server then refuses can be amended", async () => {
  const received: string[] = [];
  const q = queue(async (operation) => {
    received.push(operation.idempotencyKey);
    return (operation.payload as { card?: string }).card
      ? { result: "done" }
      : { result: "rejected", reason: "no payment method" };
  }, memoryStorage([exhaustedCharge]));
  await q.load();

  await assert.rejects(() => q.amend("op-1", { amount: 100, card: "4242" }), /exhausted/);

  await q.retry("op-1");
  await q.flush();
  assert.equal(q.failed[0]?.parkedReason, "rejected", "this time the server answered");

  const amended = await q.amend("op-1", { amount: 100, card: "4242" });
  const report = await q.flush();

  assert.equal(report.sent, 1);
  assert.notEqual(amended.idempotencyKey, "key-abc", "the fix is a request of its own");
  assert.deepEqual(received, ["key-abc", "key-1"]);
});

// An amendment is a write before it is anything else: a fix the device could
// not record must not be reported as made.
test("an amendment the device could not store says so", async () => {
  const cells = memoryStorage();
  const device = sealable(cells);
  const api = server();
  const q = queue(api.send, device.storage);
  const note = await q.enqueue("note", { title: "" });
  await q.flush();

  device.seal();
  await assert.rejects(() => q.amend(note.id, { title: "Dinner" }), /disk full/);

  const stored = (await cells.load())[0];
  assert.equal(stored?.state, "failed", "the device still holds the change that was refused");
  assert.equal(stored?.parkedReason, "rejected");
  assert.deepEqual(stored?.payload, { title: "" }, "under the payload the server answered");
  assert.equal(stored?.idempotencyKey, note.idempotencyKey, "and the key it answered it under");
});

// A read that failed is not knowledge that the device holds nothing, and
// amending is a write: it reads again rather than writing over what it never saw.
test("an amendment over a failed read touches nothing", async () => {
  const cells = memoryStorage([rejectedNote]);
  const api = server();
  const q = queue(api.send, throwsOnceOnRead(cells));

  await assert.rejects(() => q.amend("op-2", { title: "Dinner" }), /storage unavailable/);
  assert.deepEqual((await cells.load())[0]?.payload, { title: "" }, "nothing was written");
  assert.equal((await cells.load())[0]?.state, "failed");

  const amended = await q.amend("op-2", { title: "Dinner" });

  assert.deepEqual(amended.payload, { title: "Dinner" }, "the next call reads again");
  assert.equal((await cells.load())[0]?.state, "pending");
  assert.notEqual((await cells.load())[0]?.idempotencyKey, "key-note");
});
