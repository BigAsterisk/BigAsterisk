# BigDebug — watchpoints on intermediate data

A distributed job gives you the final answer and nothing in between. BigDebug's
**on-demand watchpoint** is the distributed counterpart of a watchpoint in a
conventional debugger: instead of inspecting a variable at a line of code, you place a
guard on the records flowing through a point in a query, and see which ones match —
without collecting the intermediate dataset.

Introduced in *BigDebug: Debugging Primitives for Interactive Big Data Processing in
Spark* (ICSE 2016).

!!! info "What is implemented"
    **Watchpoints** are implemented, for Spark SQL and PySpark. BigDebug's other
    primitives — simulated breakpoints, crash-culprit determination, fine-grained
    latency alerts — are **not yet** available; see
    [below](#the-other-bigdebug-primitives).

## Using it

```scala
import org.apache.spark.sql.functions.col
import org.bigasterisk.api.BigAsterisk

val orders = spark.table("orders")
val wp = BigAsterisk.watchpoints(spark).watch(orders, col("amount") > 10000)

// build the rest of the query on the instrumented DataFrame
wp.df.groupBy("cid").sum("amount").collect()

println(s"${wp.hits} suspicious rows")
wp.captured.foreach(println)
```

```python
import bigasterisk
from pyspark.sql.functions import col

wp = bigasterisk.watchpoints(spark).watch(spark.table("orders"), col("amount") > 10000)
wp.df.groupBy("cid").sum("amount").collect()

print("%d suspicious rows" % wp.hits)
for row in wp.captured:
    print(row)
```

**Build the rest of your query on `wp.df`**, not on the DataFrame you passed to
`watch` — the instrumented DataFrame is the one carrying the guard.

## What it costs

The guard becomes a branch inside Spark's whole-stage-generated code, so a row that
does not match costs one predicate evaluation and nothing else. A row is materialised
only when the guard actually matches, and only the first `capacity` matches are shipped
to the driver.

`hits` counts **every** match, including ones beyond `capacity`, so a guard that turns
out to select a billion rows tells you so instead of silently reporting the sample
size. `truncated` says whether `captured` is a sample.

```scala
val wp = watchpoints.watch(orders, col("amount") > 100, capacity = 3)
wp.df.collect()
wp.hits          // 8  — the true count
wp.captured      // 3  — what came back
wp.truncated     // true
```

Observations travel by accumulator, so they survive shuffles downstream of the
watchpoint, and Spark merges updates only from tasks that succeed — speculative and
retried attempts do not double-count. Re-running the same query **does** add to the
previous totals, because that is how accumulators work; call `reset()` between runs.

## Captured rows keep the schema you watched

Column pruning would normally narrow a scan to whatever the rest of the query needs. A
watchpoint holds on to every column of the DataFrame it watches, so a watchpoint on a
three-column table feeding an aggregation over two of them still reports three-column
rows. Showing the rows of the thing you actually watched matters more than pruning a
column out of it.

## Limitations

- **Spark Connect.** The guard is planned into the driver-side physical plan, which a
  Connect client does not build. Classic sessions only.
- **Retained rows live on the driver.** Keep `capacity` small.
- **Accumulators accumulate.** Re-running a query without `reset()` adds to the totals.

## The other BigDebug primitives

The ICSE 2016 paper describes four primitives. Watchpoints port cleanly because a guard
is just an expression and accumulators are a sanctioned executor-to-driver channel. The
others were built on machinery that only exists in a forked Spark:

| Primitive | Status | Why |
|---|---|---|
| On-demand watchpoints | **implemented** | guard as a Catalyst expression, matches via accumulator |
| Simulated breakpoints | not yet | the original pauses tasks through a forked executor backend (`BDExecutorBackend`/`BDDriverBackend`) that intercepts task execution |
| Crash culprit determination | not yet | needs the same task-level interception to catch and report the record in flight |
| Fine-grained latency alerts | not yet | overlaps with [PerfDebug](https://github.com/BigAsterisk/BigAsterisk/blob/main/PROVENANCE.md), which is scheduled separately |

These are tracked as re-architecture rather than as a port: reproducing them without
forking Spark means rebuilding them on supported extension points, not translating the
original code. See
[PROVENANCE.md](https://github.com/BigAsterisk/BigAsterisk/blob/main/PROVENANCE.md).

## Relationship to the published tool

The original shipped predicate **bytecode** to executors, hot-loaded it through a custom
class loader, and streamed matches back over RPC messages added to Spark's
`CoarseGrainedClusterMessages`. All three needed a forked Spark.

Here the guard is an ordinary Catalyst expression — which Spark already serializes with
the plan, so no bytecode shipping — and matches travel by `AccumulatorV2`, which is
Spark's own executor-to-driver channel. The observable behaviour is the same; the
mechanism is one a stock Spark supports.
