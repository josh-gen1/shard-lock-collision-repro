import { ClusterSchema, Entity, RunnerAddress } from "@effect/cluster"
import { NodeClusterSocket } from "@effect/platform-node"
import { Rpc } from "@effect/rpc"
import { PgClient } from "@effect/sql-pg"
import {
  Duration,
  Effect,
  Layer,
  Logger,
  LogLevel,
  ManagedRuntime,
  Option,
  Redacted,
  Schema
} from "effect"

const DB_URL = "postgres://postgres:pw@127.0.0.1:55432/postgres"
const SHARDS_PER_GROUP = 5

const SqlLive = PgClient.layer({ url: Redacted.make(DB_URL) })

const Counter = Entity.make("Counter", [
  Rpc.make("Ping", {
    success: Schema.String,
    payload: { msg: Schema.String }
  })
]).annotate(ClusterSchema.ShardGroup, (entityId: string) =>
  entityId.startsWith("bravo:") ? "bravo" : "alpha"
)

const CounterLive = Counter.toLayer(
  Effect.succeed({
    Ping: ({ payload }: { payload: { msg: string } }) =>
      Effect.succeed(`pong: ${payload.msg}`)
  })
)

const makeRunner = (groups: ReadonlyArray<string>, port: number) =>
  CounterLive.pipe(
    Layer.provideMerge(
      NodeClusterSocket.layer({
        storage: "sql",
        shardingConfig: {
          runnerAddress: Option.some(RunnerAddress.make("localhost", port)),
          shardGroups: groups,
          shardsPerGroup: SHARDS_PER_GROUP
        }
      })
    ),
    Layer.provide(SqlLive),
    Layer.provide(Logger.minimumLogLevel(LogLevel.Error))
  )

const tryPing = (entityId: string) =>
  Effect.gen(function*() {
    const makeClient = yield* Counter.client
    const client = makeClient(entityId)
    return yield* client.Ping({ msg: entityId }).pipe(
      Effect.timeout(Duration.seconds(5))
    )
  }).pipe(
    Effect.either,
    Effect.map((result) =>
      result._tag === "Right"
        ? `  ✅ ${entityId} → ${result.right}`
        : `  ❌ ${entityId} → ${result.left._tag === "TimeoutException" ? "TIMED OUT (worker not processing)" : `failed: ${String(result.left)}`}`
    )
  )

const main = async () => {
  console.log("\n=== Two cluster runners, disjoint shardGroups (published @effect/cluster@0.58.2) ===\n")

  const runtimeA = ManagedRuntime.make(makeRunner(["alpha"], 34431))
  const runtimeB = ManagedRuntime.make(makeRunner(["bravo"], 34432))

  try {
    await runtimeA.runtime()
    await runtimeB.runtime()
    await new Promise((r) => setTimeout(r, 3000))

    console.log("From Runner A, pinging entities in each group:")
    console.log(await runtimeA.runPromise(tryPing("alpha:hello")))
    console.log(await runtimeA.runPromise(tryPing("bravo:hello")))
  } finally {
    await Promise.all([runtimeA.dispose(), runtimeB.dispose()])
  }
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
