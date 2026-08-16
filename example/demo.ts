/**
 * A phone goes into a tunnel with three unsent changes, one of which the server
 * will never accept.
 *
 *   npm run demo
 */

import { memoryStorage, OfflineQueue, type Operation, type Outcome } from "../src/index.ts";

let online = false;
const arrived: string[] = [];

async function send(operation: Operation): Promise<Outcome> {
  if (!online) return { result: "retry", reason: "no connection" };
  if (operation.kind === "note" && !(operation.payload as { title?: string }).title) {
    return { result: "rejected", reason: "title is required" };
  }
  arrived.push(`${operation.kind} (${operation.idempotencyKey})`);
  return { result: "done" };
}

const storage = memoryStorage();
let clock = new Date("2026-08-16T10:00:00Z");
const queue = () =>
  new OfflineQueue({ storage, send, now: () => clock, random: () => 0.5, newId: nextId });

let counter = 0;
function nextId() {
  return `id-${++counter}`;
}

const offlineRun = queue();
await offlineRun.enqueue("photo", { name: "beach.jpg" });
await offlineRun.enqueue("note", { title: "" });
await offlineRun.enqueue("note", { title: "Dinner" });

console.log("in a tunnel");
let report = await offlineRun.flush();
console.log(`  sent=${report.sent} retrying=${report.retrying} rejected=${report.rejected}`);
console.log(`  outstanding: ${offlineRun.outstanding.length}`);
console.log(`  next attempt in ${offlineRun.delayFor(1)} ms`);

console.log("\nthe app is killed by the OS, and reopened five minutes later");
clock = new Date("2026-08-16T10:05:00Z");
online = true;
const restarted = queue();
await restarted.load();
report = await restarted.flush();
console.log(`  sent=${report.sent} retrying=${report.retrying} rejected=${report.rejected}`);
console.log(`  arrived at the server: ${arrived.join(", ")}`);

console.log("\nparked for the user to deal with:");
for (const operation of restarted.failed) {
  console.log(`  ${operation.kind}: ${operation.lastError} (after ${operation.attempts} attempt)`);
}
console.log("\n  the rejected note did not hold up the good one behind it");
