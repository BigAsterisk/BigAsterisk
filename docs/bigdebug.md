# BigDebug — watchpoints on intermediate data

A distributed job gives you the final answer and nothing in between. BigDebug's
**on-demand watchpoint** is the distributed counterpart of a watchpoint in a
conventional debugger: instead of inspecting a variable at a line of code, you place a
guard on the records flowing through a point in a query, and see which ones match —
without collecting the intermediate dataset.

Introduced in *BigDebug: Debugging Primitives for Interactive Big Data Processing in
Spark* (ICSE 2016).

!!! info "What is implemented"
    **Watchpoints** and **crash-culprit determination** are implemented, for Spark SQL
    and PySpark. Simulated breakpoints are **not**; see
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

## Crash-culprit determination

A failing Spark job reports a stack trace and a task id. Neither says which of the
billion records being processed was the one the code could not handle, and bisecting the
input by hand is exactly the work this removes.

```scala
val guard = BigAsterisk.crashCulprit(spark).guard(orders)
try guard.df.selectExpr("oid", "100 DIV (amount - 99999)").collect()
catch { case _: Exception => println(guard.culprit.get) }
```

```python
guard = bigasterisk.crash_culprit(spark).guard(orders)
try:
    guard.df.selectExpr("oid", "100 DIV (amount - 99999)").collect()
except Exception:
    print(guard.culprit)
```

```
partition 0, record 7: [o8,c2,99999]
  SparkArithmeticException: [DIVIDE_BY_ZERO] Division by zero...
```

The record is reported with its partition and its position in that task, so it can be
found by position as well as by value. As with watchpoints, the culprit keeps every
column of the guarded DataFrame even when the rest of the query needed only some.

Per record this writes the row into a reused buffer and assigns a field — no allocation,
and nothing moves to the driver unless the task actually dies. The row is copied only in
the failure listener.

The record reported is the last one the guard emitted, which is the culprit whether the
exception is thrown at the guard or anywhere downstream in the same pipeline, since a
stage processes one record at a time.

!!! warning "Not across a batched Python UDF"
    A Python or Arrow UDF takes a whole batch to another process before any of it can
    fail. By the time the exception comes back the guard has already emitted every row
    of that batch, so the record it remembers is the last of the batch, not the one that
    failed. Guarding *above* the UDF does not help: the guard would never see the
    failing batch at all.

    Unlike [PerfDebug](perfdebug.md), this cannot be detected and reported — a profile
    is compromised by a batched operator *below* it, which is in the plan it holds; a
    guard is compromised by one *above* it, which is whatever you go on to build.
    Express the failing computation in SQL when you need the record named.

## The other BigDebug primitives

The ICSE 2016 paper describes four primitives. Watchpoints port cleanly because a guard
is just an expression and accumulators are a sanctioned executor-to-driver channel. The
others were built on machinery that only exists in a forked Spark:

| Primitive | Status | Why |
|---|---|---|
| On-demand watchpoints | **implemented** | guard as a Catalyst expression, matches via accumulator |
| Crash culprit determination | **implemented** | a plan operator remembers the record in flight; an accumulator registered to survive task failure carries it back |
| Fine-grained latency alerts | **implemented**, as [PerfDebug](perfdebug.md) | per-record cost at a chosen point |
| Simulated breakpoints | not yet | the original pauses tasks through a forked executor backend (`BDExecutorBackend`/`BDDriverBackend`) that intercepts task execution. Pausing a live task and resuming it needs a driver-executor channel that stock Spark does not offer |

Breakpoints are tracked as re-architecture rather than as a port: reproducing them
without forking Spark means rebuilding them on supported extension points, not
translating the original code. See
[PROVENANCE.md](https://github.com/BigAsterisk/BigAsterisk/blob/main/PROVENANCE.md).

## Relationship to the published tool

The original shipped predicate **bytecode** to executors, hot-loaded it through a custom
class loader, and streamed matches back over RPC messages added to Spark's
`CoarseGrainedClusterMessages`. All three needed a forked Spark.

Here the guard is an ordinary Catalyst expression — which Spark already serializes with
the plan, so no bytecode shipping — and matches travel by `AccumulatorV2`, which is
Spark's own executor-to-driver channel. The observable behaviour is the same; the
mechanism is one a stock Spark supports.

Crash-culprit determination rests on one further detail. Accumulators are normally
merged only from tasks that *succeed*, which is precisely the wrong behaviour when the
interesting task is the one that died — but Spark supports merging updates from failed
tasks through a flag on registration, which is how its own task metrics survive a
failure. That flag is what makes the primitive possible without the forked executor
backend.
