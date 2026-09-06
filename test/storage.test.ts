import { test } from "node:test";
import assert from "node:assert/strict";

import {
  keyValueStorage,
  memoryStorage,
  mmkvStorage,
  OfflineQueue,
  type KeyValueStore,
  type Operation,
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

/**
 * Both real adapters answer the same interface, so they answer the same tests.
 * An adapter that passes these is one the queue can be trusted to run on.
 */
const adapters: ReadonlyArray<{
  name: string;
  open: (cells: Cells, key?: string) => Storage;
  unreadable: () => Storage;
  unwritable: () => Storage;
}> = [
  {
    name: "AsyncStorage",
    open: (cells, key) => keyValueStorage(asyncStoreOver(cells), key),
    unreadable: () =>
      keyValueStorage({ getItem: async () => "{not json", setItem: async () => {} }),
    unwritable: () => keyValueStorage(asyncStoreOver(new Map(), true)),
  },
  {
    name: "MMKV",
    open: (cells, key) => mmkvStorage(mmkvOver(cells), key),
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
