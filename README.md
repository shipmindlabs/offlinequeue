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

## The six states of a change

`state` holds four values, and two of them split in two: six states a screen can
tell apart, and no others. The split is the part worth knowing — a change that
is waiting on a backoff and one that is due need different words, and a refused
change and an exhausted one need different buttons.

```
  enqueue   persisted before it resolves        ──►  pending

  pending   the flush reaches it                ──►  in-flight

  in-flight outcome done                        ──►  done
  in-flight outcome retry, attempts left        ──►  waiting
  in-flight outcome retry, attempts spent       ──►  exhausted   (parked)
  in-flight outcome rejected                    ──►  rejected    (parked)
  in-flight lease run out, recover()            ──►  pending

  waiting   its stored time passes              ──►  pending

  rejected  retry(id)                           ──►  pending
  exhausted retry(id)                           ──►  pending
  rejected  discard(id)                         ──►  gone
  exhausted discard(id)                         ──►  gone
  done      prune()                             ──►  gone
```

`gone` is not a state; it is the row no longer being in storage.

| Name | How it is stored | What it is |
|---|---|---|
| pending | `pending`, nothing in `nextAttemptAt` that is still ahead | due: the next flush attempts it |
| waiting | `pending`, with `nextAttemptAt` in the future | its backoff has not elapsed |
| in-flight | `in-flight`, with `startedAt` | an attempt is on the wire — or died with the process that started it |
| rejected | `failed`, `parkedReason: "rejected"` | the server will never accept it; a person decides |
| exhausted | `failed`, `parkedReason: "exhausted"` | out of `maxAttempts`; a better network may be all it needs |
| done | `done` | accepted, and kept until `prune()` |

`queue.outstanding` is the five that are not `done`, which is what a "pending
changes" badge counts. `queue.failed` is the two parked ones.

**Into the queue.** `enqueue` writes the row before it resolves, so the change
is on the device before any request exists. Repeating an idempotency key is not
a transition at all: `enqueue` hands back whichever row holds that key, in
whatever state it is in — including `done` and both parked ones.

**pending → in-flight.** The flush takes a lane's oldest change first, sets
`startedAt`, and stores the row before the request leaves. A row that still
looked untried while its request was out is exactly how a crash becomes a
duplicate charge.

**in-flight → done, waiting, rejected or exhausted.** The outcome your `send`
returns decides which, and `maxAttempts` decides between the last two. A row
leaving `in-flight` always loses its `startedAt`, so the lease ends with the
attempt; `done` and both parked states carry no `nextAttemptAt`, because nothing
is coming for them. A rejection parks the change and the rest of its lane
carries on; a retry stops the lane, so later changes to the same entity are not
attempted ahead of it.

**waiting → pending.** By time passing, and by nothing else. `nextAttemptAt` is
a stored time read by the next flush rather than a scheduled callback, so a
backoff set before the operating system suspended the app is still in force when
the app comes back, and a restart is not a way around it.

**in-flight → pending.** Only once the lease has run out, and only through
`load()` or `recover()`. Inside the lease the attempt is assumed to still be
running, and the lane stops rather than sending a second copy. `attempts` does
not go up: an attempt whose outcome nobody recorded is not spent, so being
killed mid-request never parks a change. A row in flight with no `startedAt` has
an unknown age, and is reclaimed on sight.

**rejected or exhausted → pending.** `retry(id)` only, with `attempts` back to
zero and `parkedReason` cleared, under the original idempotency key. `discard(id)`
is the other way out. Nothing else moves a parked row — and nothing moves a
`done` one, whose only remaining event is `prune()`.

**A cancelled flush moves nothing** beyond the attempt already on the wire,
whose outcome is recorded as usual. Everything it did not reach stays `pending`,
including the rest of the lane it stopped in.

### What it refuses to do

**No CRDT, no conflict resolution, no merge.** A payload is bytes this queue
stores and sends unchanged. There is no vector clock, no operational transform,
no last-writer-wins rule over two versions of a note — because the queue sees
one device and a conflict needs two. Which version wins is the server's
decision, and the queue's job is to deliver the change in order, with a key the
server can recognise.

**It does not decide what a `409` means.** Your `send` does: `rejected` parks it
for a person, `retry` tries again under the same key. Only the application knows
whether the server's version can be reconciled, so the library refuses to guess.
A hook for that answer is on the list below, not in here.

**It does not rewrite or combine changes.** Two edits to one note are two
requests, in the order the user made them; they are never collapsed into one, and
a parked change carries exactly the payload that was enqueued. A screen that
wants one row rather than a history should enqueue under a key derived from the
draft — see below.

**It does not promise exactly-once.** It promises at-least-once on the wire and
a stable key on every attempt. Applying that key once is the server's half of
the bargain, and no client can keep it alone.

**It does not order changes to different entities.** Two lanes have no relation,
so nothing says which of them reaches the server first. If two changes must
arrive in order, they must name the same entity.

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
