/**
 * The process dies between the send and the ack.
 *
 * This is the worst moment for a mutation queue to be interrupted: the request
 * reached the server and was applied, but the answer never got back, so the
 * device has no idea whether the change happened. Here that moment is made
 * literal — the bytes in storage are snapshotted while the request is out, and
 * the next process is given nothing but those bytes, exactly as a cold start
 * after the operating system killed the app.
 *
 * What has to be true afterwards: the change is still there, it is sent again
 * under the key it already carried, and the server applies it once.
 */

import { test } from "node:test";
import assert from "node:assert/strict";

import { keyValueStorage, OfflineQueue, type Operation, type Outcome } from "../src/index.ts";

const at = (iso: string) => () => new Date(iso);

const STORAGE_KEY = "offlinequeue";
const CHARGE = "charge:42";
const OTHER = "charge:43";

/** The bytes on the device: all a killed process leaves behind. */
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

/** A transport that can read the device while its own request is still out. */
type Wire = (operation: Operation, snapshot: () => Snapshot) => Promise<Outcome>;

const sendsNothing: Wire = async () => {
  throw new Error("this process makes no requests");
};

/**
 * A server that upserts on the idempotency key: it records every request it
 * receives, and a key it has seen before is recognised rather than applied a
 * second time.
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

/** One run of the app, over the bytes the previous run left on the device. */
function run(from: Snapshot, clock: string, send: Wire) {
  const device = disk(from);
  const queue = new OfflineQueue({
    storage: device.storage,
    send: (operation) => send(operation, device.snapshot),
    now: at(clock),
    random: () => 0.5,
  });
  return { queue, snapshot: device.snapshot };
}

/** A device holding one unsent charge, as the process that recorded it left it. */
async function enqueued(): Promise<Snapshot> {
  const app = run(undefined, "2026-08-16T10:00:00Z", sendsNothing);
  await app.queue.enqueue("charge", { amount: 100 }, CHARGE);
  return app.snapshot();
}

/**
 * Flush until one request is out on the wire, then kill the process: the server
 * receives and applies it, and the snapshot returned is the device as it stood
 * at that instant. Anything the abandoned process does afterwards is written to
 * a device nobody reads again, and never reaches the server.
 */
async function killedMidUpload(
  server: Ledger,
  from: Snapshot,
  clock: string,
  dieOn = CHARGE,
): Promise<Snapshot> {
  let killed: Snapshot;
  let dead = false;
  const app = run(from, clock, async (operation, snapshot) => {
    if (dead) return { result: "retry", reason: "the process is gone" };
    if (operation.idempotencyKey === dieOn) {
      killed = snapshot();
      dead = true;
    }
    return server.accept(operation);
  });
  await app.queue.load();
  await app.queue.flush();
  return killed;
}

test("a process killed between send and ack leaves the change stored in flight", async () => {
  const server = ledger();

  const killed = await killedMidUpload(server, await enqueued(), "2026-08-16T10:00:00Z");

  assert.deepEqual(server.applied, [CHARGE], "the server did receive the charge");
  const stored = rows(killed);
  assert.equal(stored.length, 1, "the change is on the device, not only in a dead process");
  assert.equal(stored[0]?.state, "in-flight");
  assert.equal(stored[0]?.startedAt, "2026-08-16T10:00:00.000Z", "the lease the next run reads");
  assert.equal(stored[0]?.idempotencyKey, CHARGE, "and the key it will be repeated under");
  assert.deepEqual(stored[0]?.payload, { amount: 100 });
});

// The request may still be on the wire: the process that started it is gone,
// but the one that was killed could also have been a mere crash of the UI.
test("the next process does not resend while the lease is still live", async () => {
  const server = ledger();
  const killed = await killedMidUpload(server, await enqueued(), "2026-08-16T10:00:00Z");

  const next = run(killed, "2026-08-16T10:00:30Z", async (operation) => server.accept(operation));
  await next.queue.load();
  const report = await next.queue.flush();

  assert.equal(server.received.length, 1, "an attempt inside its lease is not started again");
  assert.equal(report.retrying, 1);
  assert.equal(next.queue.outstanding[0]?.state, "in-flight");
});

test("past the lease the change is recovered, repeated, and applied once", async () => {
  const server = ledger();
  const killed = await killedMidUpload(server, await enqueued(), "2026-08-16T10:00:00Z");

  const later = run(killed, "2026-08-16T10:05:00Z", async (operation) => server.accept(operation));
  await later.queue.load();
  assert.equal(later.queue.outstanding[0]?.state, "pending", "the dead attempt was reclaimed");
  assert.equal(later.queue.outstanding[0]?.startedAt, undefined);

  const report = await later.queue.flush();

  assert.deepEqual(server.received, [CHARGE, CHARGE], "the repeat carries the original key");
  assert.deepEqual(server.applied, [CHARGE], "one charge, not two");
  assert.equal(report.sent, 1);
  assert.equal(later.queue.operations.length, 1, "and one row, not two");
  assert.equal(later.queue.operations[0]?.state, "done");
  assert.deepEqual(later.queue.operations[0]?.payload, { amount: 100 });
});

test("two deaths in a row still end with one row and one charge", async () => {
  const server = ledger();
  const once = await killedMidUpload(server, await enqueued(), "2026-08-16T10:00:00Z");
  const twice = await killedMidUpload(server, once, "2026-08-16T10:05:00Z");

  const alive = run(twice, "2026-08-16T10:10:00Z", async (operation) => server.accept(operation));
  await alive.queue.load();
  await alive.queue.flush();

  assert.equal(server.received.length, 3, "three requests, because two answers were lost");
  assert.deepEqual(server.applied, [CHARGE], "the key is what makes the repeats harmless");
  assert.equal(alive.queue.operations.length, 1);
  assert.equal(alive.queue.operations[0]?.state, "done");
  // An attempt whose outcome nobody recorded is not spent: the limit counts the
  // failures the queue knows about, so a row is never parked for being killed.
  assert.equal(alive.queue.operations[0]?.attempts, 1);
});

test("a change the server acked before the kill is not sent again", async () => {
  const server = ledger();
  const first = run(undefined, "2026-08-16T10:00:00Z", sendsNothing);
  await first.queue.enqueue("charge", { amount: 100 }, CHARGE);
  await first.queue.enqueue("charge", { amount: 250 }, OTHER);

  const killed = await killedMidUpload(server, first.snapshot(), "2026-08-16T10:00:00Z", OTHER);
  const stored = rows(killed);
  assert.equal(stored[0]?.state, "done", "the first was acked before the process died");
  assert.equal(stored[1]?.state, "in-flight");

  const next = run(killed, "2026-08-16T10:05:00Z", async (operation) => server.accept(operation));
  await next.queue.load();
  await next.queue.flush();

  assert.deepEqual(server.received, [CHARGE, OTHER, OTHER], "only the unacked one went again");
  assert.deepEqual(server.applied, [CHARGE, OTHER]);
  assert.equal(next.queue.outstanding.length, 0);
});

// The screen comes back after the restart and enqueues its draft again, which
// must not become a second row for a change already on its way.
test("re-enqueueing after the kill hands back the recovered row", async () => {
  const server = ledger();
  const killed = await killedMidUpload(server, await enqueued(), "2026-08-16T10:00:00Z");

  const next = run(killed, "2026-08-16T10:05:00Z", async (operation) => server.accept(operation));
  await next.queue.load();
  const again = await next.queue.enqueue("charge", { amount: 100 }, CHARGE);

  assert.equal(again.id, rows(killed)[0]?.id, "the row that survived, not a new one");
  assert.equal(next.queue.operations.length, 1);

  await next.queue.flush();
  assert.deepEqual(server.applied, [CHARGE], "still one charge");
});
