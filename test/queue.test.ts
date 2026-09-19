import { test } from "node:test";
import assert from "node:assert/strict";

import {
  memoryStorage,
  OfflineQueue,
  keyValueStorage,
  type FlushReport,
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
  leaseMs?: number;
}) {
  return new OfflineQueue({
    storage: options.storage ?? memoryStorage(),
    send: options.send,
    now: options.now ?? at("2026-08-16T10:00:00Z"),
    random: () => 0.5,
    maxAttempts: options.maxAttempts ?? 8,
    ...(options.leaseMs === undefined ? {} : { leaseMs: options.leaseMs }),
  });
}

/** A row the process died on, optionally carrying the lease it held. */
function interruptedCharge(startedAt?: string): Operation {
  return {
    id: "op-1",
    kind: "charge",
    payload: { amount: 100 },
    idempotencyKey: "key-abc",
    createdAt: "2026-08-16T09:00:00.000Z",
    attempts: 1,
    state: "in-flight",
    ...(startedAt === undefined ? {} : { startedAt }),
  };
}

test("an accepted change is sent once and marked done", async () => {
  const wire = transport({ note: { result: "done" } });
  const q = queue({ send: wire.send });

  await q.enqueue("note", { text: "hello" });
  const report = await q.flush();

  assert.deepEqual(report, { sent: 1, rejected: 0, exhausted: 0, retrying: 0, remaining: false });
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

// The key is made on the device, so a counter that restarts with the process
// would hand the server one key for two unrelated changes.
test("every change gets a key of its own, across processes", async () => {
  const storage = memoryStorage();
  const first = queue({ storage, send: async () => ({ result: "retry", reason: "offline" }) });
  const a = await first.enqueue("note", { text: "one" });
  const b = await first.enqueue("note", { text: "two" });

  const second = queue({ storage, send: async () => ({ result: "done" }) });
  const c = await second.enqueue("note", { text: "three" });

  const keys = new Set([a.idempotencyKey, b.idempotencyKey, c.idempotencyKey]);
  assert.equal(keys.size, 3, "a restart must not reissue a key already spent");
  for (const key of keys) assert.ok(key.length >= 8, "a key the server can tell apart");
});

// A double tap on save, or an effect that runs twice on a remount.
test("enqueueing the same key twice is a no-op", async () => {
  const storage = memoryStorage();
  const wire = transport({ note: { result: "done" } });
  const q = queue({ storage, send: wire.send });

  const first = await q.enqueue("note", { title: "Dinner" }, "note:42");
  const second = await q.enqueue("note", { title: "Dinner" }, "note:42");

  assert.equal(second.id, first.id, "the row already queued is handed back");
  assert.equal(q.outstanding.length, 1);
  assert.deepEqual(
    (await storage.load()).map((operation) => operation.payload),
    [{ title: "Dinner" }],
  );

  await q.flush();
  assert.equal(wire.seen.length, 1, "the server sees one change");
});

test("a repeated key changes nothing, so nothing is written", async () => {
  const cells = memoryStorage();
  let writes = 0;
  const storage: Storage = {
    load: () => cells.load(),
    save: async (operations) => {
      writes++;
      await cells.save(operations);
    },
  };

  const q = queue({ storage, send: async () => ({ result: "done" }) });
  await q.enqueue("note", { title: "Dinner" }, "note:42");
  assert.equal(writes, 1);

  await q.enqueue("note", { title: "Dinner" }, "note:42");
  assert.equal(writes, 1);
});

test("the dedupe survives the app being killed", async () => {
  const storage = memoryStorage();
  const first = queue({ storage, send: async () => ({ result: "retry", reason: "offline" }) });
  await first.enqueue("note", { title: "Dinner" }, "note:42");

  // A new process, and a screen that enqueues the same edit on mount.
  const second = queue({ storage, send: async () => ({ result: "done" }) });
  await second.enqueue("note", { title: "Dinner" }, "note:42");

  assert.equal((await storage.load()).length, 1, "the change is not queued a second time");
  assert.equal(second.outstanding.length, 1);
});

// The duplicate charge, arranged from the client side this time.
test("a key the server already accepted is not queued again", async () => {
  const wire = transport({ charge: { result: "done" } });
  const q = queue({ send: wire.send });

  await q.enqueue("charge", { amount: 100 }, "charge:42");
  await q.flush();
  await q.enqueue("charge", { amount: 100 }, "charge:42");
  await q.flush();

  assert.equal(wire.seen.length, 1, "the charge is not applied twice");
  assert.equal(q.operations.length, 1);
});

// The ways out of the parking lane are retry() and discard(), not a second
// enqueue that would quietly do nothing while looking like it worked.
test("enqueueing a parked key hands back the parked row", async () => {
  const wire = transport({ note: { result: "rejected", reason: "title is required" } });
  const q = queue({ send: wire.send });
  await q.enqueue("note", { title: "" }, "note:42");
  await q.flush();

  const again = await q.enqueue("note", { title: "Dinner" }, "note:42");

  assert.equal(again.state, "failed");
  assert.equal(again.lastError, "title is required");
  assert.equal(q.operations.length, 1);
});

test("the key generator is injectable", async () => {
  const q = new OfflineQueue({
    storage: memoryStorage(),
    send: async () => ({ result: "done" }),
    now: at("2026-08-16T10:00:00Z"),
    random: () => 0.5,
    newKey: () => "from-the-app",
  });

  const operation = await q.enqueue("note", {});
  assert.equal(operation.idempotencyKey, "from-the-app");
});

// An operation that was in flight when the process died has an unknown fate.
// It must be retried, and the idempotency key is what makes that safe.
test("an interrupted attempt is retried under the same idempotency key", async () => {
  const wire = transport({ charge: { result: "done" } });
  const q = queue({ storage: memoryStorage([interruptedCharge()]), send: wire.send });

  await q.load();
  await q.flush();

  assert.equal(wire.seen.length, 1);
  assert.equal(wire.seen[0]?.idempotencyKey, "key-abc", "the server must recognise the repeat");
});

// The lease clock: without a start time there is no way to tell a request that
// is still out from one that died with its process.
test("an attempt in flight records when it started", async () => {
  const storage = memoryStorage();
  let duringSend: Operation | undefined;
  const q = queue({
    storage,
    send: async () => {
      duringSend = (await storage.load())[0];
      return { result: "done" };
    },
  });

  await q.enqueue("note", {});
  await q.flush();

  assert.equal(duringSend?.state, "in-flight");
  assert.equal(duringSend?.startedAt, "2026-08-16T10:00:00.000Z");
  assert.equal(q.operations[0]?.startedAt, undefined, "the lease ends with the attempt");
});

test("an attempt that outlived its lease is returned to pending on cold start", async () => {
  const storage = memoryStorage([interruptedCharge("2026-08-16T09:50:00.000Z")]);
  const wire = transport({ charge: { result: "done" } });
  const q = queue({ storage, send: wire.send });

  await q.load();

  assert.equal(q.outstanding[0]?.state, "pending");
  assert.equal(q.outstanding[0]?.startedAt, undefined, "the dead lease is not kept");
  assert.equal((await storage.load())[0]?.state, "pending", "the recovery was written down");

  await q.flush();
  assert.equal(wire.seen[0]?.idempotencyKey, "key-abc");
  assert.equal(wire.seen[0]?.attempts, 1, "recovery is not itself an attempt");
});

// A request still on the wire must not be sent a second time by a flush that
// happens to run beside it: that is the duplicate charge, arranged by hand.
test("an attempt still inside its lease is left alone", async () => {
  const storage = memoryStorage([interruptedCharge("2026-08-16T09:59:30.000Z")]);
  const wire = transport({ charge: { result: "done" } });
  const q = queue({ storage, send: wire.send });

  await q.load();
  assert.equal(q.outstanding[0]?.state, "in-flight");

  const report = await q.flush();
  assert.equal(wire.seen.length, 0, "a live attempt is not started again");
  assert.equal(report.retrying, 1);
});

// Recovery runs on every cold start, foreground and reconnect, so running it
// twice has to be the same as running it once.
test("recovery is idempotent", async () => {
  const cells = memoryStorage([interruptedCharge("2026-08-16T09:50:00.000Z")]);
  let writes = 0;
  const storage: Storage = {
    load: () => cells.load(),
    save: async (operations) => {
      writes++;
      await cells.save(operations);
    },
  };

  const q = queue({ storage, send: async () => ({ result: "done" }) });
  await q.load();
  assert.equal(writes, 1, "the reclaim was persisted once");
  assert.equal(await q.recover(), 0, "a second pass finds nothing left to reclaim");
  assert.equal(writes, 1, "and writes nothing");

  const next = queue({ storage, send: async () => ({ result: "done" }) });
  await next.load();
  assert.equal(next.outstanding[0]?.state, "pending");
  assert.equal(next.outstanding[0]?.attempts, 1, "a second cold start changes nothing");
  assert.equal(writes, 1);
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

// One draw for the whole queue would bring every change back at the same
// instant: the thundering herd, in miniature, inside one device.
test("the jitter is drawn per item", async () => {
  const draws = [0, 1];
  const q = new OfflineQueue({
    storage: memoryStorage(),
    send: async () => ({ result: "retry", reason: "offline" }),
    now: at("2026-08-16T10:00:00Z"),
    random: () => draws.shift() ?? 0.5,
  });

  await q.enqueue("note", {});
  await q.enqueue("photo", {});
  await q.flush();

  const start = Date.parse("2026-08-16T10:00:00Z");
  const delays = q.outstanding.map((o) => Date.parse(o.nextAttemptAt!) - start);
  assert.deepEqual(delays, [500, 1000], "two changes that failed together do not return together");
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
  assert.equal(q.failed[0]?.parkedReason, "rejected");
});

// A row parked after a few failures must not also carry a next attempt: the
// screen would promise a retry that is never coming.
test("parking a rejection clears the delay it was waiting on", async () => {
  const wire = transport({
    note: [
      { result: "retry", reason: "offline" },
      { result: "rejected", reason: "title is required" },
    ],
  });
  let clock = new Date("2026-08-16T10:00:00Z");
  const q = queue({ send: wire.send, now: () => clock });

  await q.enqueue("note", { title: "" });
  await q.flush();
  assert.ok(q.outstanding[0]?.nextAttemptAt, "it is waiting after the first failure");

  clock = new Date("2026-08-16T10:05:00Z");
  await q.flush();

  assert.equal(q.failed[0]?.parkedReason, "rejected");
  assert.equal(q.failed[0]?.nextAttemptAt, undefined, "a parked row is not also waiting");
  assert.equal(q.failed[0]?.attempts, 2);
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

// A delay that doubles for ever is still a queue that never stops trying.
test("an operation gives up after the attempt limit", async () => {
  const storage = memoryStorage();
  let clock = new Date("2026-08-16T10:00:00Z");
  const wire = transport({ note: { result: "retry", reason: "offline" } });
  let last: FlushReport | undefined;

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
    last = await q.flush();
    clock = new Date(clock.getTime() + 60 * 60 * 1000);
  }

  assert.equal(last?.exhausted, 1, "giving up is not the same as being refused");
  assert.equal(last?.rejected, 0);

  const final = new OfflineQueue({ storage, send: wire.send, now: () => clock, random: () => 0.5 });
  await final.load();
  assert.equal(final.failed.length, 1);
  assert.equal(final.failed[0]?.attempts, 3);
  assert.equal(final.failed[0]?.parkedReason, "exhausted", "the screen can say which it was");
  assert.equal(final.failed[0]?.lastError, "offline");
  assert.equal(final.failed[0]?.nextAttemptAt, undefined, "a parked row is not also waiting");
});

// The exit from the parking lane: the user fixes the title, or the network
// comes back, and the change goes round again under its original key.
test("a parked change can be put back in the queue", async () => {
  const wire = transport({
    note: [{ result: "rejected", reason: "title is required" }, { result: "done" }],
  });
  const q = queue({ send: wire.send });
  const enqueued = await q.enqueue("note", { title: "" });
  await q.flush();
  assert.equal(q.failed.length, 1);

  await q.retry(enqueued.id);

  assert.equal(q.failed.length, 0);
  assert.equal(q.outstanding[0]?.state, "pending");
  assert.equal(q.outstanding[0]?.attempts, 0, "the limit applies to the new run");
  assert.equal(q.outstanding[0]?.parkedReason, undefined);

  const report = await q.flush();
  assert.equal(report.sent, 1);
  assert.equal(
    wire.seen[1]?.idempotencyKey,
    wire.seen[0]?.idempotencyKey,
    "the server still sees one change, not two",
  );
});

test("only a parked change can be put back", async () => {
  const wire = transport({ note: { result: "done" } });
  const q = queue({ send: wire.send });
  const enqueued = await q.enqueue("note", {});
  await q.flush();

  await q.retry(enqueued.id);

  assert.equal(q.operations[0]?.state, "done", "an accepted change is not resurrected");
  assert.equal(wire.seen.length, 1);
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
