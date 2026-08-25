# PerfDebug — which records cost too much

Data skew — one key having far more rows than the others — is well understood and
visible in Spark's own metrics. **Computation skew** is not: a handful of records can
be orders of magnitude more expensive to *process* than the rest, and no per-task
metric says which ones. PerfDebug measures cost at record granularity, so the expensive
records can be named.

From *PerfDebug: Performance Debugging of Computation Skew in Dataflow Systems*
(SoCC 2019).

## Using it

```scala
import org.bigasterisk.api.BigAsterisk

val profile = BigAsterisk.perfdebug(spark).profile(orders, topK = 10)

// build the rest of the query on the instrumented DataFrame
profile.df.groupBy("cid").sum("amount").collect()

println(f"skew: ${profile.skew}%.1fx the mean over ${profile.records} records")
profile.slowest.foreach(println)
```

```python
import bigasterisk

profile = bigasterisk.perfdebug(spark).profile(orders, top_k=10)
profile.df.groupBy("cid").sum("amount").collect()

print("skew: %.1fx" % profile.skew)
for record in profile.slowest:
    print(record)
```

```
skew: 812.4x the mean over 11 records
61.238 ms  [o8,c2,99999]
 0.094 ms  [o4,c1,310]
```

**Build the rest of your query on `profile.df`**, not on the DataFrame you passed to
`profile` — the instrumented one is what carries the clock.

## What is measured

The clock is read once per record inside Spark's generated code. The interval between
consecutive records is the work the upstream pipeline did for the later one — which is
the quantity computation skew is about: not how long a *task* took, but which record
inside it was expensive.

Two details make the numbers mean what they say:

- **Upstream expressions are forced before the clock is read.** Spark's codegen emits an
  input variable's code at its first use, so without this a costly UDF would not run
  until the row was materialised further down, and its cost would land on the *next*
  record.
- **The first record of each task is counted but never retained.** The interval before
  it spans pipeline start-up rather than any record's own work, and would otherwise
  always look like the most expensive record in the query.

Only the `topK` most expensive records are materialised and shipped to the driver. The
totals are exact — every record is counted and its cost summed — so `meanNanos` and
`skew` are exact even though `slowest` is a sample.

## Which input made *this* output expensive

A profile says which records were expensive. Often the question is narrower: one output
row is slow and the rest are not, and you want the inputs responsible for that one.

```scala
val profile = BigAsterisk.perfdebug(spark).profile(withExpensiveStep, topK = 20)
val totals = profile.df.groupBy("cid").sum("amount")
totals.collect()

profile.blame(totals, "cid = 'c2'").foreach(println)
// 61.238 ms  [o8,c2,99999]
```

```python
profile.blame(totals, "cid = 'c2'")
```

This needs timing *and* provenance: the query is run once more with capture on, each
selected output is traced back to the records behind it, and those are matched against
what the profile measured. Only records among the profile's `slowest` can be reported,
which is the point — the question is which inputs carry the largest cost, not what every
input cost.

## Where to put the profiling point

**Above the work you want measured.** Within a fused pipeline the interval before a
record covers the work done for the *previous* one, so a point placed below an expensive
operation charges its cost to the record that follows.

```scala
// measures the UDF
perfdebug.profile(orders.withColumn("x", slowUdf(col("amount"))))

// does not: the UDF runs above the profiling point
perfdebug.profile(orders).df.withColumn("x", slowUdf(col("amount")))
```

## Record-level attribution has a boundary

`profile.recordLevel` is **false** when a batched operator sits below the profiling
point — a Python or Arrow UDF, which computes a whole batch in one call to another
process.

In that case the cost of the batch lands on whichever record triggered it rather than on
the record that caused it, so `slowest` must not be read as naming the expensive record.
Totals can also *understate*, because a batch is computed before its first output row
appears and the first record of a task is never retained.

Profile below the batched operator, or use a JVM-side expression, when record-level
attribution is what you need. The tool reports this rather than leaving you to infer it.

## Limitations

- **Overhead.** One `nanoTime` call, a subtraction and a comparison per record. That is
  cheap but not free, and it is on by default only for the DataFrame you profiled.
- **Attribution reaches only the profile's retained records.** Raise `topK` if a
  suspected culprit is not among them.
- **Accumulators accumulate.** Re-running a query without `reset()` adds to the totals.
- **Retained records live on the driver.** Keep `topK` small.
- **Spark Connect.** The timing operator is planned into the driver-side physical plan,
  which a Connect client does not build. Classic sessions only.

## Relationship to the published tool

The original propagated a latency value alongside every record through a forked Spark's
lineage machinery, and stored the results in **Apache Ignite**. Both were consequences
of needing to carry per-record state across stage boundaries in Spark 1.x/2.x.

Here the timing is taken inside Spark's generated code and travels back by
`AccumulatorV2`, so there is no forked Spark and no external store. The measurement is
taken at a point you choose rather than carried alongside every record through every
stage; attribution back to the inputs of a particular result is done with provenance at
the point of asking, rather than by propagation. See
[PROVENANCE.md](https://github.com/BigAsterisk/BigAsterisk/blob/main/PROVENANCE.md).
