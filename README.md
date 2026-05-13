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

`repro.ts` exercises the **published `@effect/cluster@0.58.2`** through the
high-level pattern for segregating workloads across runners:

- Each "pod" sets a disjoint `ShardingConfig.shardGroups` list.
- An `Entity` is annotated with `ClusterSchema.ShardGroup` so its messages
  route to a specific group based on the `entityId`.
- `Entity.client` dispatches RPCs through `Sharding` to whichever runner owns
  the target shard.

The script stands up two `NodeClusterSocket.layer` runners in one process,
on different ports, against the same Postgres:

| Runner | port  | `shardGroups` |
| ------ | ----- | ------------- |
| A      | 34431 | `["alpha"]`   |
| B      | 34432 | `["bravo"]`   |

Both register the same `Counter` entity, which routes `bravo:*` entityIds to
group `"bravo"` and everything else to `"alpha"`. From Runner A, the script
sends a `Ping` RPC to `alpha:hello` and to `bravo:hello`, each with a
5-second timeout.

If the documented pattern worked, both pings would return. On
`@effect/cluster@0.58.2`, the `bravo` ping times out: Runner B is locked
out of all its shards by the advisory-lock collision in `SqlRunnerStorage`
(both runners ask Postgres for lock numbers `1_000_001..1_000_005`, so the
second one is shut out), so no runner is processing messages for the
`bravo` group.

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
=== Two cluster runners, disjoint shardGroups (published @effect/cluster@0.58.2) ===

From Runner A, pinging entities in each group:
  ✅ alpha:hello → pong: alpha:hello
  ❌ bravo:hello → TIMED OUT (worker not processing)
```

> Note: if you re-run against a Postgres that already has the `cluster_*`
> tables from a previous run, drop them first
> (`DROP TABLE IF EXISTS cluster_runners, cluster_locks, cluster_messages, cluster_replies CASCADE`).

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
