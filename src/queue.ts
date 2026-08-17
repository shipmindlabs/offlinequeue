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

export type Operation<T = unknown> = {
  readonly id: string;
  /** What kind of change this is. The caller's vocabulary, not this library's. */
  readonly kind: string;
  readonly payload: T;
  /**
   * Sent with the request so the server can recognise a repeat. Without this,
   * a retried timeout becomes a duplicate charge.
   */
  readonly idempotencyKey: string;
  readonly createdAt: string;
  readonly attempts: number;
  readonly state: OperationState;
  /** Why it last failed, kept for the screen that shows the user. */
  readonly lastError?: string;
  /** Earliest time to try again, from the backoff. */
  readonly nextAttemptAt?: string;
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
  /** First retry delay in milliseconds. Doubles each attempt. */
  readonly baseDelayMs?: number;
  /** Ceiling for the backoff, so a long outage does not push retries to hours. */
  readonly maxDelayMs?: number;
  /** After this many attempts an operation is parked as failed. */
  readonly maxAttempts?: number;
  readonly now?: () => Date;
  /** Injectable so a test is not at the mercy of a random. */
  readonly random?: () => number;
  readonly newId?: () => string;
};

export type FlushReport = {
  readonly sent: number;
  readonly rejected: number;
  readonly retrying: number;
  /** True when something is still waiting, whether now or after a delay. */
  readonly remaining: boolean;
};

export class OfflineQueue {
  #operations: Operation[] = [];
  #options: Required<QueueOptions>;
  #loaded = false;
  #counter = 0;

  constructor(options: QueueOptions) {
    this.#options = {
      baseDelayMs: 1000,
      maxDelayMs: 5 * 60 * 1000,
      maxAttempts: 8,
      now: () => new Date(),
      random: Math.random,
      newId: () => `op-${++this.#counter}`,
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

  /** Operations parked for a person to deal with. */
  get failed(): readonly Operation[] {
    return this.#operations.filter((o) => o.state === "failed");
  }

  /** Read what survived the app being killed. */
  async load(): Promise<void> {
    if (this.#loaded) return;
    const stored = await this.#options.storage.load();
    // An operation left in flight when the process died has an unknown fate:
    // the server may or may not have applied it. It goes back to pending, and
    // the idempotency key is what makes that safe.
    this.#operations = stored.map((operation) =>
      operation.state === "in-flight" ? { ...operation, state: "pending" as const } : operation,
    );
    this.#loaded = true;
  }

  /**
   * Record a change. It is persisted before this resolves.
   *
   * Loads first when the caller has not: persisting before loading would write
   * a one-element queue over everything that survived the last run, and a
   * forgotten load() must not be a way to lose a night's changes.
   */
  async enqueue<T>(kind: string, payload: T, idempotencyKey?: string): Promise<Operation<T>> {
    await this.load();
    const operation: Operation<T> = {
      id: this.#options.newId(),
      kind,
      payload,
      idempotencyKey: idempotencyKey ?? this.#options.newId(),
      createdAt: this.#options.now().toISOString(),
      attempts: 0,
      state: "pending",
    };
    this.#operations.push(operation as Operation);
    await this.#persist();
    return operation;
  }

  /**
   * Try everything that is due, oldest first.
   *
   * Order is preserved per kind: a queue that reorders an edit and a delete
   * applies them backwards. Operations of different kinds do not block each
   * other, so one stuck upload does not hold up a note.
   */
  async flush(): Promise<FlushReport> {
    await this.load();

    const now = this.#options.now();
    let sent = 0;
    let rejected = 0;
    let retrying = 0;
    const blocked = new Set<string>();

    for (const operation of [...this.#operations]) {
      if (operation.state === "done" || operation.state === "failed") continue;
      if (blocked.has(operation.kind)) continue;
      if (operation.nextAttemptAt && new Date(operation.nextAttemptAt) > now) {
        retrying++;
        blocked.add(operation.kind);
        continue;
      }

      this.#replace(operation.id, { state: "in-flight" });
      await this.#persist();

      const outcome = await this.#attempt(operation);
      if (outcome.result === "done") {
        this.#replace(operation.id, { state: "done", attempts: operation.attempts + 1 });
        sent++;
        continue;
      }

      if (outcome.result === "rejected") {
        // Parked, not retried: the server will never accept it, and holding the
        // rest of the queue behind it helps nobody.
        this.#replace(operation.id, {
          state: "failed",
          attempts: operation.attempts + 1,
          lastError: outcome.reason,
        });
        rejected++;
        continue;
      }

      const attempts = operation.attempts + 1;
      if (attempts >= this.#options.maxAttempts) {
        this.#replace(operation.id, { state: "failed", attempts, lastError: outcome.reason });
        rejected++;
        continue;
      }

      this.#replace(operation.id, {
        state: "pending",
        attempts,
        lastError: outcome.reason,
        nextAttemptAt: new Date(now.getTime() + this.delayFor(attempts)).toISOString(),
      });
      retrying++;
      // Later operations of the same kind wait, so ordering survives.
      blocked.add(operation.kind);
    }

    await this.#persist();
    return { sent, rejected, retrying, remaining: this.outstanding.length > 0 };
  }

  /**
   * Exponential backoff with jitter.
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
    this.#operations = this.#operations.map((operation) =>
      operation.id === id ? { ...operation, ...changes } : operation,
    );
  }

  async #persist(): Promise<void> {
    await this.#options.storage.save(this.#operations);
  }
}
