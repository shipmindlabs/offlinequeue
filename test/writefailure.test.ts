/**
 * The write that failed after the server said yes.
 *
 * A flush records an attempt before it leaves, then records its outcome when
 * it lands. The second of those two writes is the one that can fail on a full
 * disk, and it fails at the worst moment: the server has applied the change,
 * and the only place that knows it is a process that may be about to die.
 *
 * So the failure travels — a flush that could not write the ack down does not
 * report it as sent — and the disk is left holding exactly what it was last
 * told: the row in flight, under its lease, with the key it went out under.
 * The next run reads that and does the only safe thing: nothing while the
 * lease is live, and one repeat under the same key once it has run out, which
 * the server recognises rather than applying twice.
 */

import { test } from "node:test";
import assert from "node:assert/strict";

import { keyValueStorage, OfflineQueue, type Operation, type Outcome, type Storage } from "../src/index.ts";

const at = (iso: string) => () => new Date(iso);

const STORAGE_KEY = "offlinequeue";
const CHARGE = "charge:42";

/** The bytes on the device: all a process leaves behind. */
type Snapshot = string | undefined;

function disk(from: Snapshot) {
  const cells = new Map<string, string>();
  if (from !== undefined) cells.set(STORAGE_KEY, from);
  return {
    storage: keyValueStorage({
      getItem: async (key: string) => cells.get(key) ?? null,
      setItem: async (key: string, value: string) => {
        cells.set(key, value);
      },
    }),
    snapshot: (): Snapshot => cells.get(STORAGE_KEY),
  };
}

function rows(snapshot: Snapshot): Operation[] {
  return snapshot === undefined ? [] : (JSON.parse(snapshot) as Operation[]);
}

/**
 * A store that loses one write: the first one asked of it after `breakOnce`,
 * and no others. A disk that is full for ever is a different test; this one is
 * about the single write that did not land.
 */
function losesOneWrite(inner: Storage) {
  let armed = false;
  const storage: Storage = {
    load: () => inner.load(),
    save: async (operations) => {
      if (armed) {
        armed = false;
        throw new Error("disk full");
      }
      await inner.save(operations);
    },
  };
  return {
    storage,
    breakOnce: () => {
      armed = true;
    },
  };
}

/**
 * A server that upserts on the idempotency key: every request is recorded, and
 * a key it has already answered is recognised rather than applied again.
 */
function ledger() {
  const received: string[] = [];
  const applied: string[] = [];
  return {
    received,
    applied,
    accept(operation: Operation): Outcome {
      received.push(operation.idempotencyKey);
      if (!applied.includes(operation.idempotencyKey)) applied.push(operation.idempotencyKey);
      return { result: "done" };
    },
  };
}

type Ledger = ReturnType<typeof ledger>;

/** One run of the app over the bytes the previous run left, on a working disk. */
function run(from: Snapshot, clock: string, server: Ledger) {
  const device = disk(from);
  const queue = new OfflineQueue({
    storage: device.storage,
    send: async (operation) => server.accept(operation),
    now: at(clock),
    random: () => 0.5,
  });
  return { queue, snapshot: device.snapshot };
}

/**
 * One charge, accepted by the server, whose ack the disk refused to take. The
 * snapshot is the device as that flush left it.
 */
async function ackLostToDisk(server: Ledger) {
  const device = disk(undefined);
  const flaky = losesOneWrite(device.storage);
  const queue = new OfflineQueue({
    storage: flaky.storage,
    send: async (operation) => {
      const answer = server.accept(operation);
      // The server has it. The next thing the queue does is write that down.
      flaky.breakOnce();
      return answer;
    },
    now: at("2026-08-16T10:00:00Z"),
    random: () => 0.5,
  });
  await queue.enqueue("charge", { amount: 100 }, CHARGE);
  await assert.rejects(() => queue.flush(), /disk full/);
  return { queue, snapshot: device.snapshot };
}

test("a flush that could not write the ack down fails rather than reporting success", async () => {
  const server = ledger();

  const app = await ackLostToDisk(server);

  assert.deepEqual(server.applied, [CHARGE], "the server did apply the charge");
  const stored = rows(app.snapshot());
  assert.equal(stored.length, 1, "the change is still on the device");
  assert.equal(stored[0]?.state, "in-flight", "the disk holds the last thing it was told");
  assert.equal(stored[0]?.startedAt, "2026-08-16T10:00:00.000Z", "the lease the next run reads");
  assert.equal(stored[0]?.idempotencyKey, CHARGE, "and the key it would be repeated under");
  assert.equal(stored[0]?.attempts, 0, "an outcome nobody could store is not an attempt spent");
});

// The row on disk says in-flight, and nothing on the device says otherwise: a
// second request inside the lease is the duplicate charge.
test("a fresh queue over that snapshot sends nothing inside the lease", async () => {
  const server = ledger();
  const app = await ackLostToDisk(server);

  const next = run(app.snapshot(), "2026-08-16T10:00:30Z", server);
  await next.queue.load();
  const report = await next.queue.flush();

  assert.equal(server.received.length, 1, "the attempt is believed to be alive");
  assert.equal(report.retrying, 1);
  assert.equal(next.queue.outstanding[0]?.state, "in-flight");
});

test("past the lease it is repeated under the same key and applied once", async () => {
  const server = ledger();
  const app = await ackLostToDisk(server);

  const later = run(app.snapshot(), "2026-08-16T10:05:00Z", server);
  await later.queue.load();
  assert.equal(later.queue.outstanding[0]?.state, "pending", "the lease ran out and it was reclaimed");
  assert.equal(later.queue.outstanding[0]?.startedAt, undefined);

  const report = await later.queue.flush();

  assert.deepEqual(server.received, [CHARGE, CHARGE], "the repeat carries the original key");
  assert.deepEqual(server.applied, [CHARGE], "one charge, not two");
  assert.equal(report.sent, 1);
  assert.equal(later.queue.operations.length, 1, "and one row, not two");
  assert.equal(later.queue.operations[0]?.state, "done");
  assert.deepEqual(later.queue.operations[0]?.payload, { amount: 100 });
  assert.equal(rows(later.snapshot())[0]?.state, "done");
});

// The process that lost the write need not die: one full disk, one failed
// write, and the queue carries on rather than being held behind it.
test("a process that survives the failed write records the ack on its next one", async () => {
  const server = ledger();
  const app = await ackLostToDisk(server);

  const report = await app.queue.flush();

  assert.equal(server.received.length, 1, "the row it holds is already accepted");
  assert.equal(report.sent, 0);
  assert.equal(report.remaining, false);
  const stored = rows(app.snapshot());
  assert.equal(stored[0]?.state, "done", "the ack reached the disk on the write after the failure");
  assert.equal(stored[0]?.startedAt, undefined, "so it left no lease behind");
});
