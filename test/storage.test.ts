import { test } from "node:test";
import assert from "node:assert/strict";

import {
  keyValueStorage,
  memoryStorage,
  mmkvStorage,
  OfflineQueue,
  type KeyValueStore,
  type Operation,
  type Outcome,
  type QuarantineEvent,
  type Storage,
  type SyncKeyValueStore,
} from "../src/index.ts";

const at = (iso: string) => () => new Date(iso);

/** The bytes an adapter writes, so two instances can share one "device". */
type Cells = Map<string, string>;

function asyncStoreOver(cells: Cells, failToWrite = false): KeyValueStore {
  return {
    getItem: async (key: string) => cells.get(key) ?? null,
    setItem: async (key: string, value: string) => {
      if (failToWrite) throw new Error("disk full");
      cells.set(key, value);
    },
  };
}

function mmkvOver(cells: Cells, failToWrite = false): SyncKeyValueStore {
  return {
    getString: (key: string) => cells.get(key),
    set: (key: string, value: string) => {
      if (failToWrite) throw new Error("disk full");
      cells.set(key, value);
    },
  };
}

function row(id: string, kind: string): Operation {
  return {
    id,
    kind,
    payload: {},
    idempotencyKey: `key-${id}`,
    createdAt: "2026-08-16T10:00:00.000Z",
    attempts: 0,
    state: "pending",
  };
}

function queueOn(storage: Storage) {
  return new OfflineQueue({
    storage,
    send: async (): Promise<Outcome> => ({ result: "done" }),
    now: at("2026-08-16T10:00:00Z"),
    random: () => 0.5,
  });
}

/**
 * Both real adapters answer the same interface, so they answer the same tests.
 * An adapter that passes these is one the queue can be trusted to run on.
 */
const adapters: ReadonlyArray<{
  name: string;
  open: (cells: Cells, key?: string) => Storage;
  watched: (cells: Cells, onQuarantine: (event: QuarantineEvent) => void) => Storage;
  /** The same device, on a store that refuses every write. */
  sealed: (cells: Cells) => Storage;
  unreadable: () => Storage;
  unwritable: () => Storage;
}> = [
  {
    name: "AsyncStorage",
    open: (cells, key) => keyValueStorage(asyncStoreOver(cells), key),
    watched: (cells, onQuarantine) =>
      keyValueStorage(asyncStoreOver(cells), undefined, { onQuarantine }),
    sealed: (cells) => keyValueStorage(asyncStoreOver(cells, true)),
    unreadable: () =>
      keyValueStorage({ getItem: async () => "{not json", setItem: async () => {} }),
    unwritable: () => keyValueStorage(asyncStoreOver(new Map(), true)),
  },
  {
    name: "MMKV",
    open: (cells, key) => mmkvStorage(mmkvOver(cells), key),
    watched: (cells, onQuarantine) => mmkvStorage(mmkvOver(cells), undefined, { onQuarantine }),
    sealed: (cells) => mmkvStorage(mmkvOver(cells, true)),
    unreadable: () => mmkvStorage({ getString: () => "{not json", set: () => {} }),
    unwritable: () => mmkvStorage(mmkvOver(new Map(), true)),
  },
];

for (const adapter of adapters) {
  // The whole promise of the library: the change is on the device before the
  // request that might never happen.
  test(`${adapter.name}: a change is a stored row before any network call`, async () => {
    const cells: Cells = new Map();
    const storage = adapter.open(cells);
    let duringSend: Operation[] = [];
    const queue = new OfflineQueue({
      storage,
      send: async () => {
        duringSend = await storage.load();
        return { result: "done" };
      },
      now: at("2026-08-16T10:00:00Z"),
      random: () => 0.5,
    });

    await queue.enqueue("note", { text: "hello" });
    const beforeFlush = await storage.load();
    assert.equal(beforeFlush.length, 1, "enqueue resolved only after the write");
    assert.equal(beforeFlush[0]?.state, "pending");

    await queue.flush();
    assert.equal(duringSend[0]?.state, "in-flight", "the row was stored while the request was out");
  });

  test(`${adapter.name}: the queue outlives the process that wrote it`, async () => {
    const cells: Cells = new Map();
    const first = new OfflineQueue({
      storage: adapter.open(cells),
      send: async () => ({ result: "retry", reason: "offline" }),
      now: at("2026-08-16T10:00:00Z"),
      random: () => 0.5,
    });
    await first.enqueue("note", { text: "written on the train" });
    await first.flush();

    const second = new OfflineQueue({
      storage: adapter.open(cells),
      send: async () => ({ result: "done" }),
      now: at("2026-08-16T10:05:00Z"),
      random: () => 0.5,
    });
    await second.load();

    assert.equal(second.outstanding.length, 1);
    assert.deepEqual(second.outstanding[0]?.payload, { text: "written on the train" });
    assert.equal(second.outstanding[0]?.attempts, 1, "the attempt count was stored too");
  });

  test(`${adapter.name}: an unreadable store starts empty rather than refusing to start`, async () => {
    assert.deepEqual(await adapter.unreadable().load(), []);
  });

  // Swallowing a write failure would lose a change silently, which is the exact
  // failure this library exists to prevent.
  test(`${adapter.name}: a store that cannot write says so`, async () => {
    const queue = new OfflineQueue({
      storage: adapter.unwritable(),
      send: async () => ({ result: "done" }),
      now: at("2026-08-16T10:00:00Z"),
      random: () => 0.5,
    });
    await assert.rejects(() => queue.enqueue("note", {}), /disk full/);
  });

  test(`${adapter.name}: a stored value that is not a queue reads as empty`, async () => {
    const cells: Cells = new Map([["offlinequeue", JSON.stringify({ migrated: true })]]);
    assert.deepEqual(await adapter.open(cells).load(), []);
  });

  // `load` answers with operations, so an app that is told nothing cannot tell a
  // store that held something unreadable from one that held nothing at all.
  test(`${adapter.name}: bytes that do not parse are kept aside and reported`, async () => {
    const cells: Cells = new Map([["offlinequeue", "{not json"]]);
    const quarantined: QuarantineEvent[] = [];
    const storage = adapter.watched(cells, (event) => quarantined.push(event));

    assert.deepEqual(await storage.load(), []);

    assert.equal(quarantined.length, 1, "once, on the way in");
    assert.equal(quarantined[0]?.key, "offlinequeue");
    assert.equal(quarantined[0]?.quarantineKey, "offlinequeue.quarantine");
    assert.match(quarantined[0]?.error ?? "", /JSON/, "the parser's own complaint");
    assert.equal(cells.get("offlinequeue.quarantine"), "{not json", "the bytes themselves survive");
  });

  test(`${adapter.name}: valid JSON of another shape is reported as not a queue`, async () => {
    const cells: Cells = new Map([["offlinequeue", JSON.stringify({ migrated: true })]]);
    const quarantined: QuarantineEvent[] = [];
    const storage = adapter.watched(cells, (event) => quarantined.push(event));

    assert.deepEqual(await storage.load(), []);

    assert.equal(quarantined.length, 1);
    assert.equal(quarantined[0]?.error, "stored value is not an array");
    assert.equal(quarantined[0]?.quarantineKey, "offlinequeue.quarantine");
    assert.equal(cells.get("offlinequeue.quarantine"), '{"migrated":true}');
  });

  test(`${adapter.name}: nothing stored, and a queue, are not quarantined`, async () => {
    const quarantined: QuarantineEvent[] = [];
    const report = (event: QuarantineEvent) => quarantined.push(event);

    const empty: Cells = new Map();
    assert.deepEqual(await adapter.watched(empty, report).load(), []);

    const held: Cells = new Map([["offlinequeue", JSON.stringify([row("op-1", "note")])]]);
    assert.equal((await adapter.watched(held, report).load()).length, 1);

    assert.deepEqual(quarantined, [], "a first run is not a device holding something unreadable");
    assert.equal(empty.size, 0, "and a load that found nothing wrote nothing");
  });

  // The copy is what makes the next write a replacement rather than a loss: the
  // queue starts empty, and the value it started empty over is still readable.
  test(`${adapter.name}: the next write replaces bytes that survive in the quarantine key`, async () => {
    const cells: Cells = new Map([["offlinequeue", "{not json"]]);

    await queueOn(adapter.open(cells)).enqueue("note", { text: "hello" });

    assert.equal(cells.get("offlinequeue.quarantine"), "{not json", "kept, byte for byte");
    const stored = JSON.parse(cells.get("offlinequeue") ?? "null") as Operation[];
    assert.equal(stored.length, 1, "and the key holds the new queue");
    assert.deepEqual(stored[0]?.payload, { text: "hello" });
  });

  // The slot holds the only copy of an earlier unreadable value. Keeping the
  // newer bytes at its expense is the loss the quarantine exists to prevent.
  test(`${adapter.name}: an occupied quarantine key stops the load instead`, async () => {
    const cells: Cells = new Map([
      ["offlinequeue", "{not json"],
      ["offlinequeue.quarantine", "[half a queue"],
    ]);
    const queue = queueOn(adapter.open(cells));

    await assert.rejects(
      () => queue.enqueue("note", { text: "hello" }),
      /already holds an unread queue/,
    );

    assert.equal(cells.get("offlinequeue"), "{not json", "neither cell moved");
    assert.equal(cells.get("offlinequeue.quarantine"), "[half a queue");
  });

  // A copy that did not land is not a copy, so the queue does not carry on to
  // write over the value it failed to keep.
  test(`${adapter.name}: a quarantine that cannot be written leaves the bytes where they are`, async () => {
    const cells: Cells = new Map([["offlinequeue", "{not json"]]);
    const queue = queueOn(adapter.sealed(cells));

    await assert.rejects(() => queue.enqueue("note", { text: "hello" }), /disk full/);

    assert.equal(cells.get("offlinequeue"), "{not json");
    assert.equal(cells.has("offlinequeue.quarantine"), false, "and no half-kept copy was left");
  });

  test(`${adapter.name}: a device holding nothing is given no quarantine key`, async () => {
    const cells: Cells = new Map();

    await queueOn(adapter.open(cells)).enqueue("note", { text: "hello" });

    assert.deepEqual([...cells.keys()], ["offlinequeue"], "a first run quarantines nothing");
  });

  test(`${adapter.name}: a second key holds a separate queue`, async () => {
    const cells: Cells = new Map();
    const drafts = adapter.open(cells, "drafts");
    const uploads = adapter.open(cells, "uploads");

    await drafts.save([row("op-1", "note")]);

    assert.equal((await drafts.load()).length, 1);
    assert.deepEqual(await uploads.load(), [], "two queues in one app do not see each other");
  });
}

test("the in-memory adapter starts from what it was seeded with", async () => {
  const storage = memoryStorage([row("op-1", "note")]);
  assert.equal((await storage.load()).length, 1);
});

// A test adapter that hands out its own array would let a queue mutate storage
// without saving, and hide the bug the real adapters would expose.
test("the in-memory adapter does not hand out its own array", async () => {
  const storage = memoryStorage([row("op-1", "note")]);
  const loaded = await storage.load();
  loaded.pop();
  assert.equal((await storage.load()).length, 1);
});
