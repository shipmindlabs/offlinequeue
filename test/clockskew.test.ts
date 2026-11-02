/**
 * A clock that stepped back after a time was stored.
 *
 * Device clocks move backwards: a user corrects one, or the network tells the
 * phone the truth about a date it guessed on boot. Every time already written
 * is then ahead of the present by the size of the step, and a queue that
 * believes them waits out that step — a lease that will not run out for an
 * hour, a backoff that has not elapsed since yesterday.
 *
 * So a `startedAt` ahead of now is rewritten to now, and the lease runs once
 * from the corrected clock rather than being thrown away: the attempt may still
 * be on the wire. A `nextAttemptAt` further ahead than the backoff could ever
 * have set it is capped to the longest wait the policy allows.
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

/** A row left in flight by a process that recorded the future. */
const inFlightCharge = (startedAt: string): Operation => ({
  id: "op-1",
  kind: "charge",
  payload: { amount: 100 },
  idempotencyKey: "key-abc",
  createdAt: "2026-08-16T09:00:00.000Z",
  attempts: 1,
  state: "in-flight",
  startedAt,
});

/** A row waiting on a backoff, as a previous run stored it. */
const waitingNote = (nextAttemptAt: string): Operation => ({
  id: "op-2",
  kind: "note",
  payload: { text: "hello" },
  idempotencyKey: "key-note",
  createdAt: "2026-08-16T09:00:00.000Z",
  attempts: 1,
  state: "pending",
  lastError: "offline",
  nextAttemptAt,
});

function queue(
  storage: Storage,
  send: (operation: Operation) => Promise<Outcome>,
  now: () => Date,
) {
  return new OfflineQueue({ storage, send, now, random: () => 0.5 });
}

test("a lease stored in the future runs from the corrected clock", async () => {
  const cells = memoryStorage([inFlightCharge("2026-08-16T11:00:00.000Z")]);
  const sent: string[] = [];
  let clock = new Date("2026-08-16T10:00:00Z");
  const q = queue(
    cells,
    async (operation) => {
      sent.push(operation.idempotencyKey);
      return { result: "done" };
    },
    () => clock,
  );

  await q.load();

  assert.equal(q.outstanding[0]?.state, "in-flight", "the request may still be out");
  assert.equal(q.outstanding[0]?.startedAt, "2026-08-16T10:00:00.000Z", "the lease starts now");
  assert.equal(
    (await cells.load())[0]?.startedAt,
    "2026-08-16T10:00:00.000Z",
    "and the correction was written down",
  );

  const report = await q.flush();
  assert.deepEqual(sent, [], "a restarted lease is not a reclaimed one");
  assert.equal(report.retrying, 1);

  clock = new Date("2026-08-16T10:02:00Z");
  assert.equal(await q.recover(), 1, "the lease runs out once, from the corrected clock");
  assert.equal(q.outstanding[0]?.state, "pending");
  assert.equal(q.outstanding[0]?.startedAt, undefined);
});

test("a backoff stored beyond the ceiling is capped to the longest delay", async () => {
  const cells = memoryStorage([waitingNote("2026-08-17T10:00:00.000Z")]);
  const sent: string[] = [];
  let clock = new Date("2026-08-16T10:00:00Z");
  const q = queue(
    cells,
    async (operation) => {
      sent.push(operation.idempotencyKey);
      return { result: "done" };
    },
    () => clock,
  );

  await q.load();

  assert.equal(
    q.outstanding[0]?.nextAttemptAt,
    "2026-08-16T10:05:00.000Z",
    "a day ahead is not a delay the backoff could have set",
  );
  assert.equal((await cells.load())[0]?.nextAttemptAt, "2026-08-16T10:05:00.000Z");

  await q.flush();
  assert.deepEqual(sent, [], "it still waits, for as long as the policy allows");

  clock = new Date("2026-08-16T10:05:00Z");
  await q.flush();
  assert.deepEqual(sent, ["key-note"], "and no longer than that");
});

test("a clock stepped back during a session is corrected by the next flush", async () => {
  let clock = new Date("2026-08-16T11:00:00Z");
  const q = queue(
    memoryStorage(),
    async () => ({ result: "retry", reason: "offline" }),
    () => clock,
  );
  await q.enqueue("note", { text: "hello" });
  await q.flush();
  assert.equal(q.outstanding[0]?.nextAttemptAt, "2026-08-16T11:00:00.750Z");

  clock = new Date("2026-08-16T10:00:00Z");
  const report = await q.flush();

  assert.equal(report.retrying, 1, "the row is waiting, not attempted");
  assert.equal(
    q.outstanding[0]?.nextAttemptAt,
    "2026-08-16T10:05:00.000Z",
    "an hour of waiting becomes the five minutes the policy allows",
  );
  assert.equal(q.outstanding[0]?.attempts, 1, "and the correction is not an attempt");
});

// The correction must be invisible to a device whose clock never moved.
test("times the policy could have set are left alone, and nothing is written", async () => {
  const cells = memoryStorage([
    inFlightCharge("2026-08-16T09:59:30.000Z"),
    waitingNote("2026-08-16T10:00:30.000Z"),
  ]);
  let writes = 0;
  const storage: Storage = {
    load: () => cells.load(),
    save: async (operations) => {
      writes++;
      await cells.save(operations);
    },
  };

  const q = queue(storage, async () => ({ result: "done" }), () => new Date("2026-08-16T10:00:00Z"));
  await q.load();

  assert.equal(writes, 0, "a correction nobody needed is not a write");
  assert.equal(q.outstanding[0]?.startedAt, "2026-08-16T09:59:30.000Z");
  assert.equal(q.outstanding[1]?.nextAttemptAt, "2026-08-16T10:00:30.000Z");
});
