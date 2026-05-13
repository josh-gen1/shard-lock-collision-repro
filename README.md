# `@effect/cluster` shard-lock collision repro

Minimal repro for a bug in `@effect/cluster`'s `SqlRunnerStorage` (current `main`,
`packages/cluster/src/SqlRunnerStorage.ts`): two runners that declare different
`shardGroups` against the same Postgres collide on `pg_advisory_lock` numbers
and one runner is silently locked out of all its shards.

## The bug in one paragraph

`SqlRunnerStorage` computes the advisory-lock number for each shard as
`(i + 1) * 1_000_000 + shard`, where `i` is the index of the group in **this
runner's** `shardGroups` array. The lock number is therefore a per-runner-
relative value, but the locks themselves are global to the database. Two
runners that deliberately use disjoint `shardGroups` — the recommended pattern
for segregating workloads inside one cluster — both end up asking for lock
numbers in `1_000_001 … 1_000_000 + shardsPerGroup`, regardless of group name.
First runner wins, second runner gets zero shards, every workflow routed to
the second group hangs forever.

## What this repro does

`repro.ts` uses the **published `@effect/cluster@0.58.2`** directly — it
imports `SqlRunnerStorage`, `RunnerStorage`, `ShardingConfig`, etc. and wires
up two real runners against one Postgres. No hand-rolled copy of the lock
formula; the bug is reproduced by exercising the actual library code.

Each "runner" gets its own `ManagedRuntime` with:

- its own `PgClient` connection pool to the same Postgres,
- its own `ShardingConfig` providing a distinct `shardGroups` list,
- a `SqlRunnerStorage` built from those layers.

Both then call `storage.acquire(address, shardIds)` for their own group's
shards. Because the buggy formula derives the lock number from the group's
*index* in this runner's `shardGroups`, both runners ask Postgres for lock
numbers `1_000_001..1_000_005` and the second one is shut out.

## Run it

Requires Docker and Node 18+.

```bash
# 1. Postgres on an isolated port
docker run -d --name shard-lock-repro -p 55432:5432 \
  -e POSTGRES_PASSWORD=pw postgres:16

# 2. Install + run
npm install
node repro.ts

# 3. Clean up
docker rm -f shard-lock-repro
```

Expected output:

```
=== Two runners, disjoint shardGroups (published @effect/cluster@0.58.2) ===
  [Runner A] shardGroups=["alpha"] → acquired 5/5
  [Runner B] shardGroups=["bravo"] → acquired 0/5, LOST 5: bravo:1, ...
  result: A=5 shards, B=0 shards  ❌ orphaned groups: bravo
```

The lock numbers themselves don't overlap from each runner's local
perspective (each one asks for "its" range starting at index 0) — but the
*global* lock numbers they request are identical, so the second runner can
never acquire any of them.

## Proposed fix

Derive the lock number from the group **name**, not from its position in this
runner's `shardGroups`:

```ts
for (const group of config.shardGroups) {
  let h = 0x811c9dc5 >>> 0
  for (let k = 0; k < group.length; k++) {
    h = Math.imul(h ^ group.charCodeAt(k), 0x01000193)
  }
  const base = (h >>> 0) & 0xFFFFFC00 // low 10 bits reserved for shard
  for (let shard = 1; shard <= config.shardsPerGroup; shard++) {
    const shardId = ShardId.make(group, shard).toString()
    const lockNum = base + shard
    lockNumbers.set(shardId, lockNum)
    lockNumbersReverse.set(lockNum, shardId)
  }
}
```

Lock numbers become a deterministic function of `(group, shard)`, identical
across all runners. Stays inside `uint32` so the `objid` round-trip through
`pg_locks` (which stores it as OID) is lossless.
