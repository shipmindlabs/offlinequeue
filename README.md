# offlinequeue

The changes a mobile app made while it could not reach the server, kept until it
can.

Reading offline is easy: cache the response. Writing offline is where apps
break, and they break in four specific ways.

```console
$ npm run demo
in a tunnel
  sent=0 retrying=2 rejected=0
  outstanding: 3
  next attempt in 750 ms

the app is killed by the OS, and reopened five minutes later
  sent=2 retrying=0 rejected=1
  arrived at the server: photo (id-2), note (id-6)

parked for the user to deal with:
  note: title is required (after 2 attempt)

  the rejected note did not hold up the good one behind it
```

**The change lives in memory and the OS kills the app.** The edit is gone and
nothing says so. Here every change is persisted before `enqueue` resolves.

**No backoff.** A phone that regains a weak signal sends a hundred failed
requests and flattens the battery. Here the delay doubles, is capped, and
carries jitter — without which every phone that lost the same cell tower retries
at the same instant and hands the outage back to the server as a thundering
herd.

**A timeout is retried and the server applies it twice.** Two identical charges.
Every operation carries an `idempotencyKey` that survives restarts, and an
operation interrupted mid-flight is retried under the same one.

**One rejected change blocks everything.** A validation error retried for ever
with the rest of the queue behind it. Here a rejection is parked for a person to
deal with, and other work continues.

## Use

```ts
const queue = new OfflineQueue({
  storage: keyValueStorage(AsyncStorage),
  send: async (operation) => {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Idempotency-Key": operation.idempotencyKey },
      body: JSON.stringify(operation.payload),
    });
    if (response.ok) return { result: "done" };
    if (response.status >= 400 && response.status < 500) {
      return { result: "rejected", reason: await response.text() };
    }
    return { result: "retry", reason: `status ${response.status}` };
  },
});

await queue.load();
await queue.enqueue("note", { title, body });

// on reconnect, on foreground, on a timer
await queue.flush();
```

The three outcomes are the whole contract. `done` and `retry` are obvious;
**`rejected` is the one that matters** — it means the server will never accept
this, so retrying is pointless and blocking the queue behind it helps nobody.

## Storage

A change becomes a stored row before any network call, so the store is the one
piece that has to be real. Three adapters answer the same interface, and the
choice is one line:

```ts
keyValueStorage(AsyncStorage)  // anything with getItem/setItem
mmkvStorage(new MMKV())        // the same two operations, synchronous
memoryStorage()                // tests, and a first run before storage is wired
```

A second argument names the key, because two queues in one app must not share
one. Writing your own adapter is two methods, `load` and `save`.

## No React Native import

Storage and transport arrive as functions. `AsyncStorage` fits `KeyValueStore`
and MMKV fits `SyncKeyValueStore` without this package knowing either exists,
which is why every behaviour above is tested in a plain test runner rather than
needing a device.

It also means the same queue runs in a web app, a worker, or a server-side test
of your own sync logic.

## Two smaller decisions

**Order is kept within a kind, not globally.** A queue that reorders an edit and
a delete applies them backwards. But one stuck photo upload should not hold up a
note, so different kinds proceed independently.

**A store that cannot read starts empty; a store that cannot write throws.** An
unreadable queue is bad and an app that will not start is worse — but swallowing
a write failure would lose a change silently, which is the exact thing this
exists to prevent.

## Status

| | |
|---|---|
| Implemented | durable enqueue, AsyncStorage / MMKV / in-memory adapters behind one interface, exponential backoff with jitter and a ceiling, idempotency keys across restarts, rejection parking, per-kind ordering, attempt limit, prune and discard |
| Not yet | a conflict-resolution hook for `409`, batching several operations into one request, a React hook wrapping `flush` on connectivity change, encryption at rest |

## Development

```bash
npm test        # node --test, no device and no network
npm run demo
npm run typecheck
```

## License

MIT © [Shipmind Labs](https://shipmindlabs.com)
