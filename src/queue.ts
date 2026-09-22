/**
 * A durable queue of changes a phone made while it could not reach the server.
 *
 * Reading offline is easy — cache the response. Writing offline is where mobile
 * apps break, and they break in four specific ways:
 *
 *   1. The change is kept in memory, and the operating system kills the app in
 *      the background. The user's edit is gone and nothing says so.
 *   2. The retry has no backoff, so a phone that regains a weak signal sends a
 *      hundred failed requests and drains the battery.
 *   3. A request that timed out is retried, and the server, which did receive
 *      it, applies it twice. Two identical charges.
 *   4. Everything stops behind one change the server will never accept — a
 *      validation failure retried for ever, with the rest of the queue behind it.
 *
 * This is the queue and the policy. It performs no network calls and touches no
 * React Native module: storage and transport arrive as functions, which is why
 * all of the above is testable in a plain test runner.
 */

export type OperationState = "pending" | "in-flight" | "failed" | "done";

/**
 * Why a parked operation stopped. The two need different words on screen and
 * different actions from the user: a rejection wants the change edited or
 * dropped, an exhausted one often only wants a better network.
 */
export type ParkedReason = "rejected" | "exhausted";

export type Operation<T = unknown> = {
  readonly id: string;
  /** What kind of change this is. The caller's vocabulary, not this library's. */
  readonly kind: string;
  readonly payload: T;
  /**
   * Given on the device when the change was recorded, and sent with every
   * attempt so the server can upsert rather than insert. Without this, a
   * retried timeout becomes a duplicate charge. It is also what the queue
   * dedupes on: enqueueing this key again is a no-op.
   */
  readonly idempotencyKey: string;
  readonly createdAt: string;
  readonly attempts: number;
  readonly state: OperationState;
  /** Why it last failed, kept for the screen that shows the user. */
  readonly lastError?: string;
  /** Set while the operation is parked, alongside the error that parked it. */
  readonly parkedReason?: ParkedReason;
  /** Earliest time to try again, from the backoff. */
  readonly nextAttemptAt?: string;
  /**
   * When the attempt now in flight began, present only while it is in flight.
   * This is the lease clock: it is how a cold start tells an attempt that is
   * still running from one that died with the process that started it.
   */
  readonly startedAt?: string;
};

/** Somewhere the queue survives the app being killed. */
export type Storage = {
  load(): Promise<Operation[]>;
  save(operations: readonly Operation[]): Promise<void>;
};

/** How an attempt ended. The distinction is the whole point. */
export type Outcome =
  /** The server accepted it. */
  | { readonly result: "done" }
  /**
   * The server refused it and always will — a validation error, a deleted
   * record, a permission failure. Retrying is pointless and blocks everything
   * behind it.
   */
  | { readonly result: "rejected"; readonly reason: string }
  /** The attempt failed for a reason that may pass: offline, timeout, 5xx. */
  | { readonly result: "retry"; readonly reason: string };

export type Send = (operation: Operation) => Promise<Outcome>;

export type QueueOptions = {
  readonly storage: Storage;
  readonly send: Send;
  /**
   * What a change touches — the thing its order is kept against. Two changes
   * to one entity are sent in the order they were made; changes to different
   * entities never wait for each other. Defaults to the kind, which orders a
   * change against every other change of its kind.
   */
  readonly entityOf?: (operation: Operation) => string;
  /**
   * How many entities may be flushed at once. Because ordering is per entity,
   * this limits unrelated changes in flight together and is never a way for two
   * changes to one entity to overtake each other.
   */
  readonly concurrency?: number;
  /** First retry delay in milliseconds. Doubles each attempt. */
  readonly baseDelayMs?: number;
  /** Ceiling for the backoff, so a long outage does not push retries to hours. */
  readonly maxDelayMs?: number;
  /** After this many attempts an operation is parked as failed. */
  readonly maxAttempts?: number;
  /**
   * How long an attempt is believed to be alive. Past it, a row still in
   * flight is treated as the remains of a process that died mid-request and is
   * returned to pending. Longer than the slowest request the transport allows.
   */
  readonly leaseMs?: number;
  readonly now?: () => Date;
  /** Injectable so a test is not at the mercy of a random. */
  readonly random?: () => number;
  readonly newId?: () => string;
  /**
   * How a change gets its idempotency key when the caller supplies none.
   * Injectable for tests, and for an app that already has a client id for the
   * thing being changed.
   */
  readonly newKey?: () => string;
};

export type FlushReport = {
  readonly sent: number;
  /** Refused by the server, and parked for that reason. */
  readonly rejected: number;
  /** Parked for running out of attempts rather than for being refused. */
  readonly exhausted: number;
  readonly retrying: number;
  /** True when something is still waiting, whether now or after a delay. */
  readonly remaining: boolean;
};

/** The running count of one flush, filled in by the lanes as they go. */
type Tally = { sent: number; rejected: number; exhausted: number; retrying: number };

/**
 * A key for one change on one device.
 *
 * A counter is wrong here even though it reads better: it restarts at one with
 * the process, so a second run of the app would hand the server a key it has
 * already seen, and the upsert would quietly drop a change that was not a
 * repeat at all.
 */
function deviceKey(): string {
  const source: { randomUUID?: () => string } | undefined = globalThis.crypto;
  if (typeof source?.randomUUID === "function") return source.randomUUID();
  const part = () => Math.random().toString(36).slice(2, 10);
  return `${Date.now().toString(36)}-${part()}${part()}`;
}

export class OfflineQueue {
  #operations: Operation[] = [];
  #options: Required<QueueOptions>;
  #loaded = false;
  #counter = 0;
  #writing: Promise<void> = Promise.resolve();

  constructor(options: QueueOptions) {
    this.#options = {
      entityOf: (operation) => operation.kind,
      concurrency: 4,
      baseDelayMs: 1000,
      maxDelayMs: 5 * 60 * 1000,
      maxAttempts: 8,
      leaseMs: 2 * 60 * 1000,
      now: () => new Date(),
      random: Math.random,
      newId: () => `op-${++this.#counter}`,
      newKey: deviceKey,
      ...options,
    };
  }

  get operations(): readonly Operation[] {
    return this.#operations;
  }

  /** Everything not yet accepted, which is what a "pending changes" badge counts. */
  get outstanding(): readonly Operation[] {
    return this.#operations.filter((o) => o.state !== "done");
  }

  /**
   * Operations parked for a person to deal with, whether refused by the server
   * or out of attempts. Each carries the error that stopped it and which of
   * the two it was, which is the whole content of a "did not sync" screen.
   */
  get failed(): readonly Operation[] {
    return this.#operations.filter((o) => o.state === "failed");
  }

  /** Read what survived the app being killed, and reclaim what it interrupted. */
  async load(): Promise<void> {
    if (this.#loaded) return;
    this.#operations = await this.#options.storage.load();
    this.#loaded = true;
    await this.recover();
  }

  /**
   * Return abandoned attempts to pending, and answer how many were reclaimed.
   *
   * An operation left in flight has an unknown fate: the server may or may not
   * have applied it. Until the lease runs out the attempt is assumed to still
   * be running, so it is not sent a second time; after it, the attempt is taken
   * to have died with its process and the row goes back to pending, where the
   * idempotency key is what makes sending it again safe.
   *
   * Reclaiming changes nothing but the state, so a second pass finds nothing
   * and writes nothing. Safe to call on every cold start, foreground or
   * reconnect.
   */
  async recover(): Promise<number> {
    await this.load();
    const now = this.#options.now().getTime();
    const stuck = this.#operations.filter(
      (operation) => operation.state === "in-flight" && this.#leaseExpired(operation, now),
    );
    for (const operation of stuck) {
      this.#replace(operation.id, { state: "pending", startedAt: undefined });
    }
    if (stuck.length > 0) await this.#persist();
    return stuck.length;
  }

  /**
   * Record a change. It is persisted before this resolves.
   *
   * The change is given a key on the device unless the caller passes one, and
   * enqueueing a key the queue already holds does nothing but hand back the row
   * already there. So a key derived from what the user edited —
   * `note:${draftId}` — makes a double tap on save, or an effect that runs
   * twice on a remount, one change rather than two.
   *
   * Loads first when the caller has not: persisting before loading would write
   * a one-element queue over everything that survived the last run, and a
   * forgotten load() must not be a way to lose a night's changes.
   */
  async enqueue<T>(kind: string, payload: T, idempotencyKey?: string): Promise<Operation<T>> {
    await this.load();
    const key = idempotencyKey ?? this.#options.newKey();
    // Accepted rows count too: one that reached the server is exactly the change
    // that must not be queued a second time.
    const existing = this.#operations.find((operation) => operation.idempotencyKey === key);
    if (existing) return existing as Operation<T>;

    const operation: Operation<T> = {
      id: this.#options.newId(),
      kind,
      payload,
      idempotencyKey: key,
      createdAt: this.#options.now().toISOString(),
      attempts: 0,
      state: "pending",
    };
    this.#operations.push(operation as Operation);
    await this.#persist();
    return operation;
  }

  /**
   * Try everything that is due.
   *
   * Changes to one entity are a lane: attempted oldest first, and the lane
   * stops at the first one that has to be tried again, because a queue that
   * reorders an edit and a delete applies them backwards. Lanes have no such
   * relation to each other, so they run together — up to `concurrency` of them,
   * which is what keeps one stuck upload from holding up anything but itself.
   */
  async flush(): Promise<FlushReport> {
    await this.load();

    const now = this.#options.now();
    const tally: Tally = { sent: 0, rejected: 0, exhausted: 0, retrying: 0 };

    const lanes = new Map<string, Operation[]>();
    for (const operation of this.#operations) {
      if (operation.state === "done" || operation.state === "failed") continue;
      const entity = this.#options.entityOf(operation);
      const waiting = lanes.get(entity);
      if (waiting) waiting.push(operation);
      else lanes.set(entity, [operation]);
    }

    const due = [...lanes.values()];
    let next = 0;
    const workers = Math.max(1, Math.min(this.#options.concurrency, due.length));
    await Promise.all(
      Array.from({ length: workers }, async () => {
        while (next < due.length) {
          await this.#runLane(due[next++]!, now, tally);
        }
      }),
    );

    await this.#persist();
    return { ...tally, remaining: this.outstanding.length > 0 };
  }

  /**
   * Exponential backoff with jitter, drawn afresh for each operation.
   *
   * The jitter is not decoration. Without it every phone that lost the same
   * cell tower retries at the same instant, and the server gets the outage back
   * as a thundering herd the moment it recovers.
   */
  delayFor(attempts: number): number {
    const exponential = this.#options.baseDelayMs * 2 ** (attempts - 1);
    const capped = Math.min(exponential, this.#options.maxDelayMs);
    return Math.round(capped * (0.5 + this.#options.random() * 0.5));
  }

  /**
   * Put a parked operation back in the queue: the user fixed what the server
   * complained about, or an exhausted change deserves another run now that the
   * network is back.
   *
   * The attempt count starts again, so the limit applies to the new run rather
   * than being already spent. The idempotency key does not change, so a change
   * the server did receive is still not applied twice.
   */
  async retry(id: string): Promise<void> {
    await this.load();
    const parked = this.#operations.find((operation) => operation.id === id);
    if (!parked || parked.state !== "failed") return;
    this.#replace(id, {
      state: "pending",
      attempts: 0,
      parkedReason: undefined,
      nextAttemptAt: undefined,
      startedAt: undefined,
    });
    await this.#persist();
  }

  /** Drop a parked operation, e.g. after the user acknowledges it. */
  async discard(id: string): Promise<void> {
    await this.load();
    this.#operations = this.#operations.filter((operation) => operation.id !== id);
    await this.#persist();
  }

  /** Forget accepted operations. Nothing else is removed. */
  async prune(): Promise<void> {
    await this.load();
    this.#operations = this.#operations.filter((operation) => operation.state !== "done");
    await this.#persist();
  }

  /**
   * One entity's changes, in the order they were made.
   *
   * Nothing here overtakes anything else: the first change that has to wait —
   * for its backoff, or because its attempt is still out on the wire — ends the
   * lane for this flush. A parked change is the exception the parking lane
   * exists for, and the lane carries on past it.
   */
  async #runLane(lane: readonly Operation[], now: Date, tally: Tally): Promise<void> {
    for (const operation of lane) {
      if (operation.state === "in-flight" && !this.#leaseExpired(operation, now.getTime())) {
        // An attempt inside its lease is still out on the wire. Sending it
        // again is the duplicate the whole idempotency story exists to avoid.
        tally.retrying++;
        return;
      }
      if (operation.nextAttemptAt && new Date(operation.nextAttemptAt) > now) {
        tally.retrying++;
        return;
      }

      this.#replace(operation.id, { state: "in-flight", startedAt: now.toISOString() });
      await this.#persist();

      const outcome = await this.#attempt(operation);
      if (outcome.result === "done") {
        this.#replace(operation.id, {
          state: "done",
          attempts: operation.attempts + 1,
          nextAttemptAt: undefined,
          startedAt: undefined,
        });
        tally.sent++;
        continue;
      }

      if (outcome.result === "rejected") {
        // Parked, not retried: the server will never accept it, and holding the
        // rest of the entity's changes behind it helps nobody.
        this.#park(operation, "rejected", outcome.reason);
        tally.rejected++;
        continue;
      }

      const attempts = operation.attempts + 1;
      if (attempts >= this.#options.maxAttempts) {
        // The limit is the other end of the backoff: a delay that doubles for
        // ever is still a queue that never stops trying.
        this.#park({ ...operation, attempts }, "exhausted", outcome.reason);
        tally.exhausted++;
        continue;
      }

      this.#replace(operation.id, {
        state: "pending",
        attempts,
        lastError: outcome.reason,
        nextAttemptAt: new Date(now.getTime() + this.delayFor(attempts)).toISOString(),
        startedAt: undefined,
      });
      tally.retrying++;
      // Later changes to this entity wait, so ordering survives.
      return;
    }
  }

  #park(operation: Operation, reason: ParkedReason, error: string): void {
    this.#replace(operation.id, {
      state: "failed",
      attempts: operation.attempts + (reason === "rejected" ? 1 : 0),
      lastError: error,
      parkedReason: reason,
      // A parked row that kept its next attempt would promise the screen a
      // retry that is never coming.
      nextAttemptAt: undefined,
      startedAt: undefined,
    });
  }

  #leaseExpired(operation: Operation, now: number): boolean {
    // A row in flight without a start time was written by a version that did
    // not record one. Its age is unknown, so it is reclaimed rather than left
    // to sit in flight for ever.
    if (!operation.startedAt) return true;
    const startedAt = new Date(operation.startedAt).getTime();
    if (Number.isNaN(startedAt)) return true;
    return now - startedAt >= this.#options.leaseMs;
  }

  async #attempt(operation: Operation): Promise<Outcome> {
    try {
      return await this.#options.send(operation);
    } catch (error) {
      // A thrown transport error is a retry, not a rejection: the request may
      // well have arrived, which is exactly what the idempotency key covers.
      return { result: "retry", reason: error instanceof Error ? error.message : String(error) };
    }
  }

  #replace(id: string, changes: Partial<Operation>): void {
    this.#operations = this.#operations.map((operation): Operation => {
      if (operation.id !== id) return operation;
      const next = { ...operation, ...changes };
      // A key set to undefined is still a key once stored: a surviving
      // `startedAt` would claim a lease and a surviving `nextAttemptAt` a
      // delay, over a row that has neither.
      const fields = next as unknown as Record<string, unknown>;
      for (const key of Object.keys(changes)) {
        if (fields[key] === undefined) delete fields[key];
      }
      return next;
    });
  }

  /**
   * Write the queue, one write at a time.
   *
   * Several lanes are in flight at once and each of them saves. Two stores
   * overlapping would let an older snapshot land last and undo a change that
   * had already been recorded, so the writes queue behind each other; each one
   * takes the queue as it is when its turn comes.
   */
  #persist(): Promise<void> {
    const written = this.#writing.then(() => this.#options.storage.save(this.#operations));
    // The caller still hears about a failed write; the next one is not held
    // behind a rejection it cannot do anything about.
    this.#writing = written.catch(() => {});
    return written;
  }
}
