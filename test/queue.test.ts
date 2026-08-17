import { test } from "node:test";
import assert from "node:assert/strict";

import {
  memoryStorage,
  OfflineQueue,
  keyValueStorage,
  type KeyValueStore,
  type Operation,
  type Outcome,
  type Storage,
} from "../src/index.ts";

const at = (iso: string) => () => new Date(iso);

/** A transport whose answers are scripted per operation kind. */
function transport(script: Record<string, Outcome | Outcome[]>) {
  const seen: Operation[] = [];
  const remaining = { ...script };
  return {
    seen,
    send: async (operation: Operation): Promise<Outcome> => {
      seen.push(operation);
      const answer = remaining[operation.kind];
      if (Array.isArray(answer)) return answer.shift() ?? { result: "done" };
      return answer ?? { result: "done" };
    },
  };
}

function queue(options: {
  storage?: Storage;
  send: (operation: Operation) => Promise<Outcome>;
  now?: () => Date;
  maxAttempts?: number;
}) {
  return new OfflineQueue({
    storage: options.storage ?? memoryStorage(),
    send: options.send,
    now: options.now ?? at("2026-08-16T10:00:00Z"),
    random: () => 0.5,
    maxAttempts: options.maxAttempts ?? 8,
  });
}

test("an accepted change is sent once and marked done", async () => {
  const wire = transport({ note: { result: "done" } });
  const q = queue({ send: wire.send });

  await q.enqueue("note", { text: "hello" });
  const report = await q.flush();

  assert.deepEqual(report, { sent: 1, rejected: 0, retrying: 0, remaining: false });
  assert.equal(wire.seen.length, 1);
  assert.equal(q.outstanding.length, 0);
});

// The failure that loses a user's edit: the change lives in memory and the OS
// kills the app in the background.
test("a change survives the app being killed", async () => {
  const storage = memoryStorage();
  const first = queue({ storage, send: async () => ({ result: "retry", reason: "offline" }) });
  await first.enqueue("note", { text: "written on the train" });
  await first.flush();

  // A new process, the same storage, some minutes later — the backoff is
  // persisted too, so a restart does not reset it into an immediate retry.
  const wire = transport({ note: { result: "done" } });
  const second = queue({ storage, send: wire.send, now: at("2026-08-16T10:05:00Z") });
  await second.load();

  assert.equal(second.outstanding.length, 1);
  await second.flush();
  assert.deepEqual(wire.seen[0]?.payload, { text: "written on the train" });
  assert.equal(second.outstanding.length, 0);
});

// The other half of that: restarting the app must not become a way to bypass
// the backoff and hammer a server that is still down.
test("a restart does not reset the backoff", async () => {
  const storage = memoryStorage();
  const first = queue({ storage, send: async () => ({ result: "retry", reason: "offline" }) });
  await first.enqueue("note", {});
  await first.flush();

  const wire = transport({ note: { result: "done" } });
  const immediately = queue({ storage, send: wire.send, now: at("2026-08-16T10:00:00Z") });
  await immediately.load();
  await immediately.flush();

  assert.equal(wire.seen.length, 0, "the delay is still in force after the restart");
});

// Persisting a one-element queue over everything that survived the last run:
// the data-loss path for a caller who forgets load() before enqueueing.
test("enqueueing before load does not overwrite the stored queue", async () => {
  const storage = memoryStorage();
  const overnight = queue({ storage, send: async () => ({ result: "retry", reason: "offline" }) });
  await overnight.enqueue("note", { text: "survived the night" });

  // A new process enqueues straight away, without calling load() first.
  const forgetful = queue({ storage, send: async () => ({ result: "done" }) });
  await forgetful.enqueue("note", { text: "fresh" });

  assert.equal((await storage.load()).length, 2, "the overnight change must still be there");
  assert.equal(forgetful.outstanding.length, 2);
});

// An operation that was in flight when the process died has an unknown fate.
// It must be retried, and the idempotency key is what makes that safe.
test("an interrupted attempt is retried under the same idempotency key", async () => {
  const interrupted: Operation = {
    id: "op-1",
    kind: "charge",
    payload: { amount: 100 },
    idempotencyKey: "key-abc",
    createdAt: "2026-08-16T09:00:00.000Z",
    attempts: 1,
    state: "in-flight",
  };
  const wire = transport({ charge: { result: "done" } });
  const q = queue({ storage: memoryStorage([interrupted]), send: wire.send });

  await q.load();
  await q.flush();

  assert.equal(wire.seen.length, 1);
  assert.equal(wire.seen[0]?.idempotencyKey, "key-abc", "the server must recognise the repeat");
});

test("the idempotency key survives every retry of the same operation", async () => {
  const wire = transport({
    charge: [{ result: "retry", reason: "timeout" }, { result: "done" }],
  });
  const q = queue({ send: wire.send });
  await q.enqueue("charge", { amount: 100 }, "key-fixed");

  await q.flush();
  await new OfflineQueue({
    storage: memoryStorage(q.operations as Operation[]),
    send: wire.send,
    now: at("2026-08-16T10:10:00Z"),
    random: () => 0.5,
  }).flush();

  assert.equal(wire.seen.length, 2);
  assert.equal(wire.seen[0]?.idempotencyKey, "key-fixed");
  assert.equal(wire.seen[1]?.idempotencyKey, "key-fixed");
});

// A hundred failed requests on a weak signal is a flat battery.
test("retries back off exponentially, with a ceiling", async () => {
  const q = queue({ send: async () => ({ result: "retry", reason: "offline" }) });

  // random() is 0.5, so the jitter multiplier is 0.75.
  assert.equal(q.delayFor(1), 750);
  assert.equal(q.delayFor(2), 1500);
  assert.equal(q.delayFor(3), 3000);
  assert.equal(q.delayFor(10), Math.round(5 * 60 * 1000 * 0.75), "capped at the maximum");
});

test("an operation not yet due is left alone", async () => {
  const wire = transport({ note: { result: "retry", reason: "offline" } });
  const q = queue({ send: wire.send });
  await q.enqueue("note", {});

  const first = await q.flush();
  assert.equal(first.retrying, 1);
  assert.equal(wire.seen.length, 1);

  // Same clock: the backoff has not elapsed.
  const second = await q.flush();
  assert.equal(wire.seen.length, 1, "nothing was sent again before the delay passed");
  assert.equal(second.retrying, 1);
});

// Everything stopping behind one change the server will never accept is the
// fourth way these queues break.
test("a rejected change is parked and does not block the rest", async () => {
  const wire = transport({
    note: { result: "rejected", reason: "title is required" },
    photo: { result: "done" },
  });
  const q = queue({ send: wire.send });
  await q.enqueue("note", { title: "" });
  await q.enqueue("photo", { name: "beach.jpg" });

  const report = await q.flush();

  assert.equal(report.rejected, 1);
  assert.equal(report.sent, 1, "the photo went through despite the note failing");
  assert.equal(q.failed.length, 1);
  assert.equal(q.failed[0]?.lastError, "title is required");
});

// A queue that reorders an edit and a delete applies them backwards.
test("order is kept within a kind while one is retrying", async () => {
  const wire = transport({ note: [{ result: "retry", reason: "offline" }] });
  const q = queue({ send: wire.send });
  await q.enqueue("note", { text: "first" });
  await q.enqueue("note", { text: "second" });

  await q.flush();

  assert.equal(wire.seen.length, 1, "the second note waits behind the first");
  assert.deepEqual(wire.seen[0]?.payload, { text: "first" });
});

test("an operation gives up after the attempt limit", async () => {
  const storage = memoryStorage();
  let clock = new Date("2026-08-16T10:00:00Z");
  const wire = transport({ note: { result: "retry", reason: "offline" } });

  for (let round = 0; round < 3; round++) {
    const q = new OfflineQueue({
      storage,
      send: wire.send,
      now: () => clock,
      random: () => 0.5,
      maxAttempts: 3,
    });
    await q.load();
    if (round === 0) await q.enqueue("note", {});
    await q.flush();
    clock = new Date(clock.getTime() + 60 * 60 * 1000);
  }

  const final = new OfflineQueue({ storage, send: wire.send, now: () => clock, random: () => 0.5 });
  await final.load();
  assert.equal(final.failed.length, 1);
  assert.equal(final.failed[0]?.attempts, 3);
});

// A thrown transport error may still have reached the server, which is exactly
// what the idempotency key covers - so it is a retry, not a rejection.
test("a transport that throws is a retry, not a rejection", async () => {
  const q = queue({
    send: async () => {
      throw new Error("network request failed");
    },
  });
  await q.enqueue("note", {});
  const report = await q.flush();

  assert.equal(report.retrying, 1);
  assert.equal(report.rejected, 0);
  assert.equal(q.outstanding[0]?.lastError, "network request failed");
});

test("done operations can be pruned and parked ones discarded", async () => {
  const wire = transport({ note: { result: "done" }, bad: { result: "rejected", reason: "nope" } });
  const q = queue({ send: wire.send });
  await q.enqueue("note", {});
  await q.enqueue("bad", {});
  await q.flush();

  await q.prune();
  assert.equal(q.operations.length, 1, "the rejected one is not pruned away");

  await q.discard(q.failed[0]!.id);
  assert.equal(q.operations.length, 0);
});

test("an unreadable store starts empty rather than refusing to start", async () => {
  const broken: KeyValueStore = {
    getItem: async () => "{not json",
    setItem: async () => {},
  };
  const storage = keyValueStorage(broken);
  assert.deepEqual(await storage.load(), []);
});

// Swallowing a write failure would lose a change silently, which is the exact
// failure this library exists to prevent.
test("a store that cannot write says so", async () => {
  const full: KeyValueStore = {
    getItem: async () => null,
    setItem: async () => {
      throw new Error("disk full");
    },
  };
  const q = queue({ storage: keyValueStorage(full), send: async () => ({ result: "done" }) });
  await assert.rejects(() => q.enqueue("note", {}), /disk full/);
});
