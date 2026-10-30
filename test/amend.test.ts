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
