/**
 * Storage adapters.
 *
 * React Native is not imported here. AsyncStorage arrives as an object with two
 * asynchronous methods, MMKV as an object with two synchronous ones, and both
 * are described by types declared in this file rather than by a dependency —
 * which is what makes the queue testable in a plain runner, and usable from a
 * web worker or a server-side test of the same logic.
 *
 * Every adapter answers the same `Storage` interface, so the choice of store is
 * one line at construction and changes nothing else. A mutation is written
 * through one of these before any request goes out, which is the only reason
 * an edit survives the operating system killing the app.
 */

import type { Operation, Storage } from "./queue.ts";

/** The shape of AsyncStorage, and of a dozen other key-value stores. */
export type KeyValueStore = {
  getItem(key: string): Promise<string | null>;
  setItem(key: string, value: string): Promise<void>;
};

/**
 * The shape of MMKV: the same two operations, synchronous, and a missing key
 * reads as `undefined` rather than `null`.
 */
export type SyncKeyValueStore = {
  getString(key: string): string | null | undefined;
  set(key: string, value: string): void;
};

/** What was found under the key instead of a queue, and where it was kept. */
export type QuarantineEvent = {
  readonly key: string;
  readonly quarantineKey: string;
  readonly error: string;
};

export type StorageOptions = {
  /**
   * Told that the value under the key was not a queue, once the bytes have been
   * copied aside. `load` answers with operations, so without this an app cannot
   * tell a store that held something unreadable from one that held nothing.
   */
  readonly onQuarantine?: (event: QuarantineEvent) => void;
};

const defaultKey = "offlinequeue";

const notAnArray = "stored value is not an array";

/** The queue found under the key, or why what was there is not one. */
type Reading = { readonly operations: Operation[]; readonly error?: string };

/**
 * Anything that is not an array of operations is not a queue. A store holding
 * something else — a key collision, a half-written value — reads as empty
 * rather than as a queue of nonsense, and the reason travels so that the bytes
 * can be kept aside rather than only dropped.
 *
 * A store that could not answer at all is a different failure and is not
 * handled here: it has said nothing about what the device holds, so it is not
 * turned into an empty queue.
 */
function decode(raw: string): Reading {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    return { operations: [], error: error instanceof Error ? error.message : String(error) };
  }
  if (!Array.isArray(parsed)) return { operations: [], error: notAnArray };
  return { operations: parsed as Operation[] };
}

/**
 * Keep the queue in an asynchronous key-value store, under one key.
 *
 * A value that is not a queue reads as empty: a key collision or a half-written
 * value is bad, but an app that will not start is worse. The bytes are copied
 * to `${key}.quarantine` and `onQuarantine` says so, so a value nobody expected
 * can still be looked at. A store that cannot be read at all is the other case,
 * and that failure travels — a read that failed is not knowledge that the
 * device holds nothing, and the queue must not write over what it never saw. A
 * failed write is not swallowed either: that would lose a change silently,
 * which is the exact failure this library exists to prevent.
 */
export function keyValueStorage(
  store: KeyValueStore,
  key = defaultKey,
  options: StorageOptions = {},
): Storage {
  const quarantineKey = `${key}.quarantine`;
  return {
    async load() {
      const raw = await store.getItem(key);
      if (!raw) return [];
      const read = decode(raw);
      if (read.error !== undefined) {
        await store.setItem(quarantineKey, raw);
        options.onQuarantine?.({ key, quarantineKey, error: read.error });
      }
      return read.operations;
    },
    async save(operations) {
      await store.setItem(key, JSON.stringify(operations));
    },
  };
}

/**
 * Keep the queue in MMKV, under one key.
 *
 * MMKV is synchronous, so the write has already happened by the time the
 * promise resolves; the asynchronous signature exists only so that the same
 * queue can also hold an AsyncStorage. Read and write failures, and a value
 * that is not a queue, are handled the same way as above, for the same reasons.
 */
export function mmkvStorage(
  store: SyncKeyValueStore,
  key = defaultKey,
  options: StorageOptions = {},
): Storage {
  const quarantineKey = `${key}.quarantine`;
  return {
    async load() {
      const raw = store.getString(key);
      if (!raw) return [];
      const read = decode(raw);
      if (read.error !== undefined) {
        store.set(quarantineKey, raw);
        options.onQuarantine?.({ key, quarantineKey, error: read.error });
      }
      return read.operations;
    },
    async save(operations) {
      store.set(key, JSON.stringify(operations));
    },
  };
}

/** An in-memory store, for tests and for a first run before storage is wired. */
export function memoryStorage(initial: readonly Operation[] = []): Storage {
  let held: Operation[] = [...initial];
  return {
    async load() {
      return [...held];
    },
    async save(operations) {
      held = [...operations];
    },
  };
}
