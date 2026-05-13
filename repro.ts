// Real-Effect repro of the @effect/cluster SqlRunnerStorage advisory-lock
// collision bug. Uses the published @effect/cluster@0.58.2 directly — no
// hand-rolled copy of the lock-number formula.
//
// Two SqlRunnerStorage instances share one Postgres. Each is provided a
// DIFFERENT ShardingConfig.shardGroups list. The current implementation
// derives the advisory-lock number from the group's INDEX in this runner's
// shardGroups array — not from the group name. So two runners that both have
// one group at index 0 ask for the same lock numbers (1_000_001..1_000_005)
// and collide: the second runner's `storage.acquire` returns zero shards.
//
// Run:
//   docker run -d --name shard-lock-repro -p 55432:5432 \
//     -e POSTGRES_PASSWORD=pw postgres:16
//   npm install
//   node repro.ts
//   docker rm -f shard-lock-repro

import {
  RunnerAddress,
  RunnerStorage,
  ShardId,
  ShardingConfig,
  SqlRunnerStorage
} from "@effect/cluster"
import { PgClient } from "@effect/sql-pg"
import { Layer, Logger, LogLevel, ManagedRuntime, Option, Redacted } from "effect"
import * as Effect from "effect/Effect"

const DB_URL = "postgres://postgres:pw@127.0.0.1:55432/postgres"
const SHARDS_PER_GROUP = 5

const SqlLive = PgClient.layer({ url: Redacted.make(DB_URL) })

// Each "runner" gets its own ManagedRuntime: its own PgClient pool, its own
// ShardingConfig (with its own shardGroups), its own SqlRunnerStorage. This
// demonstrates the SqlRunnerStorage bug: two physical runner processes both
// point at the same Postgres while declaring disjoint shardGroups.
const makeRunnerLayer = (shardGroups: ReadonlyArray<string>, port: number) => {
  const ConfigLive = ShardingConfig.layer({
    shardGroups,
    shardsPerGroup: SHARDS_PER_GROUP,
    runnerAddress: Option.some(RunnerAddress.make("localhost", port))
  })
  return SqlRunnerStorage.layer.pipe(
    Layer.provide(SqlLive),
    Layer.provide(ConfigLive)
  )
}

const acquireFor = (name: string, shardGroups: ReadonlyArray<string>, port: number) =>
  Effect.gen(function*() {
    const storage = yield* RunnerStorage.RunnerStorage
    const address = RunnerAddress.make("localhost", port)

    const shardIds: Array<ShardId.ShardId> = []
    for (const group of shardGroups) {
      for (let i = 1; i <= SHARDS_PER_GROUP; i++) {
        shardIds.push(ShardId.make(group, i))
      }
    }

    const acquired = yield* storage.acquire(address, shardIds)
    const acquiredSet = new Set(acquired.map((s) => s.toString()))
    const requested = shardIds.map((s) => s.toString())
    const lost = requested.filter((s) => !acquiredSet.has(s))

    console.log(
      `  [${name}] shardGroups=${JSON.stringify(shardGroups)} ` +
        `→ acquired ${acquired.length}/${shardIds.length}` +
        (lost.length ? `, LOST ${lost.length}: ${lost.join(", ")}` : "")
    )
    return { acquired, lost }
  })

const main = async () => {
  console.log("\n=== Two runners, disjoint shardGroups (published @effect/cluster@0.58.2) ===")

  // Each ManagedRuntime owns its own scope — its own Postgres pool, its own
  // advisory-lock connection, its own SqlRunnerStorage. They share the
  // database, not the process state.
  const runtimeA = ManagedRuntime.make(makeRunnerLayer(["alpha"], 34431))
  const runtimeB = ManagedRuntime.make(makeRunnerLayer(["bravo"], 34432))

  try {
    const a = await runtimeA.runPromise(
      acquireFor("Runner A", ["alpha"], 34431).pipe(
        Logger.withMinimumLogLevel(LogLevel.Warning)
      )
    )
    const b = await runtimeB.runPromise(
      acquireFor("Runner B", ["bravo"], 34432).pipe(
        Logger.withMinimumLogLevel(LogLevel.Warning)
      )
    )

    const orphanedGroups: Array<string> = []
    if (a.acquired.length === 0) orphanedGroups.push("alpha")
    if (b.acquired.length === 0) orphanedGroups.push("bravo")
    console.log(
      `  result: A=${a.acquired.length} shards, B=${b.acquired.length} shards` +
        (orphanedGroups.length
          ? `  ❌ orphaned groups: ${orphanedGroups.join(", ")}`
          : "  ✅ both groups owned")
    )
  } finally {
    await runtimeA.dispose()
    await runtimeB.dispose()
  }
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
