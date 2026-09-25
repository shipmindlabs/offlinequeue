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
  the second tap on save returned id-3, the row already queued
  next attempt in 750 ms

the app is killed by the OS, and reopened five minutes later
  sent=2 retrying=0 rejected=1
  arrived at the server: photo (key-1), note (key-3)

parked for the user to deal with:
  note: title is required (rejected after 2 attempts)

  the rejected note did not hold up the good one behind it
```

**The change lives in memory and the OS kills the app.** The edit is gone and
nothing says so. Here every change is persisted before `enqueue` resolves.

**No backoff.** A phone that regains a weak signal sends a hundred failed
requests and flattens the battery. Here the delay doubles, is capped, and
carries jitter drawn per change — without which every phone that lost the same
cell tower retries at the same instant and hands the outage back to the server
as a thundering herd.

**A timeout is retried and the server applies it twice.** Two identical charges.
Every change is given an `idempotencyKey` on the device when it is enqueued, and
that key survives restarts and is sent with every attempt, so the server upserts
instead of inserting. Enqueueing a key the queue already holds is a no-op, so a
double tap on save queues one change rather than two. An attempt also records
when it started, so a change still in flight when the process died is recognised
on the next start and returned to pending once its lease has run out — retried
under the same key, and not while the first request may still be on the wire.

**One rejected change blocks everything.** A validation error retried for ever
with the rest of the queue behind it. Here a rejection is parked for a person to
deal with, and other work continues. A change that merely ran out of attempts is
parked too, since a delay that doubles for ever is still a queue that never
stops trying.

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

// when the device can reach the server
await queue.flush();
```

The three outcomes are the whole contract. `done` and `retry` are obvious;
**`rejected` is the one that matters** — it means the server will never accept
this, so retrying is pointless and blocking the queue behind it helps nobody.

`load` reclaims abandoned attempts on the way in, and `recover` does the same
thing on demand; both are safe to call again, since a reclaim changes nothing
the second time. Set `leaseMs` longer than the slowest request your transport
will allow.

## One change, one key

`enqueue` generates the key unless you pass one, and it is generated here rather
than asked for, because the server may never hear about the change at all. Pass
your own when the change already has an identity on the device:

```ts
await queue.enqueue("note", { title, body }, `note:${draftId}`);
```

The queue then holds one row however many times the screen calls `enqueue` — a
double tap, an effect that runs twice on a remount, a retried submit. A repeated
key returns the row already queued rather than adding another, including one the
server has already accepted. A parked row is returned the same way, which is why
the ways out of the parking lane are `retry(id)` and `discard(id)` rather than
enqueueing again.

The key does not change when an attempt is retried or a parked change is put
back, so a repeat on the wire is a repeat the server can recognise.

## The parking lane

`queue.failed` is everything that stopped, which is what a "these did not sync"
screen lists. A parked change keeps the error that stopped it in `lastError` and
says which of the two things happened in `parkedReason`: `rejected` needs a
person, `exhausted` ran out of `maxAttempts` and may only need a better network.
A parked row carries no `nextAttemptAt`, because nothing is coming.

There are two ways out. `discard(id)` drops it, and `retry(id)` puts it back in
the queue with its attempt count reset — under the original idempotency key, so
a change the server did receive is still not applied twice.

## In order, but not one at a time

Two changes to the same thing have to arrive in the order the user made them: a
queue that reorders an edit and a delete applies them backwards. Two changes to
different things have no such relation, and one stuck photo upload should not
hold a note behind it.

So order is kept per entity, and an entity is whatever your changes touch — you
name it:

```ts
new OfflineQueue({
  storage,
  send,
  entityOf: (operation) => (operation.payload as { noteId?: string }).noteId ?? operation.kind,
  concurrency: 4,
});
```

Changes to one entity are a lane: attempted oldest first, and the lane stops at
the first change that has to be tried again rather than stepping over it. Lanes
never wait for each other — `flush` runs up to `concurrency` of them at once,
four by default, so a note goes out while a photo for another entity is still on
the wire. Lower it to one for a server that dislikes parallel writes.

A parked change is the exception parking exists for: its lane carries on past
it, because nothing is coming for that row and holding the rest back helps
nobody.

`entityOf` defaults to the kind, so a queue that names no entity behaves as it
always did — ordered within a kind, concurrent across kinds.

## On the network, not on a timer

A flush is work for the moment the phone can reach the server, so it starts on a
connectivity event and stops on the loss of one:

```ts
const stop = queue.start((listener) =>
  NetInfo.addEventListener((state) => listener(Boolean(state.isInternetReachable))),
);

// on sign-out, or when the screen that owns the queue goes away
stop();
```

There is no timer in here, and that is the point. A `setTimeout` set in the
foreground does not survive the operating system suspending the app: it never
fires, or it fires late in a batch at a moment nobody chose. So a backoff is a
stored time rather than a pending callback, and it is read by the next flush —
which the platform asks for, on reconnect or on foreground.

Losing the connection cancels the flush in progress, rather than letting it walk
the rest of the queue spending an attempt per change on requests that cannot
arrive. The attempt already on the wire is finished and its outcome recorded,
because walking away from it would leave a lease nobody closes and a row whose
fate is unknown. Everything after it stays pending.

Events arrive in bursts — coming out of a tunnel is several of them — and one
flush per event would put the same queue on the wire twice, so a burst is one
flush, then one more if something asked while it ran. A flush started from an
event has no caller to throw to; pass a second argument to `start` to hear about
a write that failed.

`flush` takes a cancel token of your own for the same reasons — a sign-out, a
screen that is going away:

```ts
const controller = new AbortController();
await queue.flush({ signal: controller.signal });
```

An `AbortSignal` fits, and so does anything else with an `aborted` boolean.
`queue.cancel()` does the same to the flush the queue started itself.

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
one. Writing your own adapter is two methods, `load` and `save`. A flush has
several lanes in flight at once, and the queue serialises its writes so two of
them never overlap: an adapter is only ever asked to save one thing at a time.

## No React Native import

Storage, transport and connectivity arrive as functions. `AsyncStorage` fits
`KeyValueStore`, MMKV fits `SyncKeyValueStore` and `NetInfo.addEventListener`
fits `Connectivity` without this package knowing any of them exists, which is
why every behaviour above is tested in a plain test runner rather than needing a
device.

It also means the same queue runs in a web app, a worker, or a server-side test
of your own sync logic.

## One smaller decision

**A store that cannot read starts empty; a store that cannot write throws.** An
unreadable queue is bad and an app that will not start is worse — but swallowing
a write failure would lose a change silently, which is the exact thing this
exists to prevent.

## Status

| | |
|---|---|
| Implemented | durable enqueue, device-generated idempotency keys that dedupe on enqueue and stay stable across restarts and retries, AsyncStorage / MMKV / in-memory adapters behind one interface, exponential backoff with per-item jitter and a ceiling, in-flight leases with idempotent recovery of abandoned attempts, an attempt limit, parking that keeps the last error and why it stopped, requeueing or discarding a parked change, ordering per entity with unrelated entities flushed concurrently under a limit, flushing on connectivity events with cancellation and no timers, prune |
| Not yet | editing the payload of a parked change before requeueing it, a conflict-resolution hook for `409`, batching several operations into one request, a React hook wrapping `start`, encryption at rest |

## Development

```bash
npm test        # node --test, no device and no network
npm run demo
npm run typecheck
```

## License

MIT © [Shipmind Labs](https://shipmindlabs.com)
