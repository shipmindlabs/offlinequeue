import { test } from "node:test";
import assert from "node:assert/strict";

import {
  memoryStorage,
  OfflineQueue,
  type Connectivity,
  type Operation,
  type Outcome,
} from "../src/index.ts";

const at = (iso: string) => () => new Date(iso);

/** Let everything pending settle, so "what is on the wire" is a stable answer. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

/** The platform's connectivity events, under the test's control. */
function connection() {
  let listeners: Array<(online: boolean) => void> = [];
  const source: Connectivity = (listener) => {
    listeners.push(listener);
    return () => {
      listeners = listeners.filter((waiting) => waiting !== listener);
    };
  };
  return {
    source,
    get listening() {
      return listeners.length;
    },
    report(online: boolean) {
      for (const listener of [...listeners]) listener(online);
    },
  };
}

/** A transport that can be held open, so a flush can be caught mid-attempt. */
function wire() {
  const sent: string[] = [];
  let held: Promise<void> | undefined;
  let open: (() => void) | undefined;
  let inFlight = 0;
  let peak = 0;
  return {
    sent,
    get peak() {
      return peak;
    },
    hold() {
      held = new Promise<void>((resolve) => {
        open = resolve;
      });
    },
    release() {
      open?.();
      held = undefined;
      open = undefined;
    },
    send: async (operation: Operation): Promise<Outcome> => {
      sent.push(operation.kind);
      inFlight++;
      peak = Math.max(peak, inFlight);
      const waiting = held;
      if (waiting) await waiting;
      inFlight--;
      return { result: "done" };
    },
  };
}

const entityOf = (operation: Operation) =>
  (operation.payload as { note?: string }).note ?? operation.kind;

function queue(send: (operation: Operation) => Promise<Outcome>, concurrency = 1) {
  return new OfflineQueue({
    storage: memoryStorage(),
    send,
    entityOf,
    concurrency,
    now: at("2026-08-16T10:00:00Z"),
    random: () => 0.5,
  });
}

test("nothing goes out until the platform says the device is online", async () => {
  const net = connection();
  const transport = wire();
  const q = queue(transport.send);
  await q.enqueue("edit", { note: "1" });

  q.start(net.source);
  await settle();
  assert.deepEqual(transport.sent, [], "listening is not flushing");

  net.report(false);
  await settle();
  assert.deepEqual(transport.sent, [], "an offline event starts nothing");

  net.report(true);
  await settle();
  assert.deepEqual(transport.sent, ["edit"]);
  assert.equal(q.outstanding.length, 0);
  q.stop();
});

// A flush that carries on into an outage spends the queue's attempts on
// requests that cannot arrive.
test("losing the connection stops the flush where it is", async () => {
  const net = connection();
  const transport = wire();
  const q = queue(transport.send);
  for (const note of ["1", "2", "3"]) await q.enqueue("edit", { note });

  transport.hold();
  q.start(net.source);
  net.report(true);
  await settle();
  assert.equal(transport.sent.length, 1, "one attempt is on the wire");

  net.report(false);
  transport.release();
  await settle();

  assert.equal(transport.sent.length, 1, "the changes behind it were not attempted");
  assert.equal(q.operations[0]?.state, "done", "the attempt on the wire was finished, not dropped");
  assert.equal(q.operations[0]?.startedAt, undefined, "so it left no lease behind");
  assert.equal(q.outstanding.length, 2);

  net.report(true);
  await settle();
  assert.equal(transport.sent.length, 3, "the rest go out when the connection returns");
  q.stop();
});

test("a flush honours a cancel token", async () => {
  const transport = wire();
  const q = queue(transport.send);
  for (const note of ["1", "2"]) await q.enqueue("edit", { note });

  const controller = new AbortController();
  transport.hold();
  const flushing = q.flush({ signal: controller.signal });
  await settle();
  controller.abort();
  transport.release();
  const report = await flushing;

  assert.equal(report.sent, 1);
  assert.equal(report.remaining, true);
  assert.equal(transport.sent.length, 1, "the second entity was left for the next flush");
});

test("a flush cancelled before it starts sends nothing", async () => {
  const transport = wire();
  const q = queue(transport.send);
  await q.enqueue("edit", { note: "1" });

  const report = await q.flush({ signal: { aborted: true } });

  assert.deepEqual(transport.sent, []);
  assert.equal(report.sent, 0);
  assert.equal(q.outstanding[0]?.state, "pending", "the row is left as it was found");
});

test("stopping unsubscribes, so a later event flushes nothing", async () => {
  const net = connection();
  const transport = wire();
  const q = queue(transport.send);
  await q.enqueue("edit", { note: "1" });

  const stop = q.start(net.source);
  stop();
  assert.equal(net.listening, 0);

  net.report(true);
  await settle();
  assert.deepEqual(transport.sent, []);
});

// Coming out of a tunnel is several events, and one flush per event would put
// the same queue on the wire twice.
test("an event during a flush queues one more, not a second at the same time", async () => {
  const net = connection();
  const transport = wire();
  const q = queue(transport.send);
  await q.enqueue("edit", { note: "1" });

  transport.hold();
  q.start(net.source);
  net.report(true);
  await settle();

  await q.enqueue("edit", { note: "2" });
  net.report(true);
  await settle();
  assert.equal(transport.sent.length, 1, "the second event did not start a flush of its own");

  transport.release();
  await settle();

  assert.equal(transport.sent.length, 2, "it ran once the first flush was done");
  assert.equal(transport.peak, 1, "never two attempts at once under a limit of one");
  assert.equal(q.outstanding.length, 0);
  q.stop();
});
