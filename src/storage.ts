/**
 * Storage adapters.
 *
 * React Native's AsyncStorage is not imported here. It arrives as an object
 * with two methods, so this package has no dependency on React Native at all —
 * which is what makes the queue testable in a plain runner, and usable from a
 * web worker or a server-side test of the same logic.
 */

import type { Operation, Storage } from "./queue.ts";

/** The shape of AsyncStorage, and of a dozen other key-value stores. */
export type KeyValueStore = {
  getItem(key: string): Promise<string | null>;
  setItem(key: string, value: string): Promise<void>;
};

/**
 * Keep the queue in a key-value store under one key.
 *
 * A store that fails to read is treated as empty rather than fatal: an
 * unreadable queue is bad, but an app that will not start is worse. A store
 * that fails to write is not swallowed — that would lose a change silently,
 * which is the exact failure this library exists to prevent.
 */
export function keyValueStorage(store: KeyValueStore, key = "offlinequeue"): Storage {
  return {
    async load() {
      try {
        const raw = await store.getItem(key);
        if (!raw) return [];
        const parsed: unknown = JSON.parse(raw);
        return Array.isArray(parsed) ? (parsed as Operation[]) : [];
      } catch {
        return [];
      }
    },
    async save(operations) {
      await store.setItem(key, JSON.stringify(operations));
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
