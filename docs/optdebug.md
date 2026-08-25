# OptDebug — which operation is at fault

Data provenance answers *which records* produced a wrong result. It says nothing about
*which part of the query* was at fault. OptDebug closes that gap: it scores a query's
operations the way spectrum-based fault localisation scores lines of code. An operation
that most failing records passed through, and few passing records did, is the one to
look at.

From *OptDebug: Fault-Inducing Operation Isolation for Dataflow Applications*
(SoCC 2021).

OptDebug isolates the fault in the **code**: which operation is responsible. It does not
report which input records are to blame.

## Using it

```scala
import org.bigasterisk.optdebug.OptDebug

val df = spark.sql(
  """SELECT cid, SUM(CASE WHEN amount > 1000 THEN -amount ELSE amount END) AS total
    |FROM orders GROUP BY cid""".stripMargin)

val result = OptDebug.localize(spark, df, (r: Row) => r.getLong(1) < 0)
result.ranked.foreach(println)
```

```python
import bigasterisk

result = bigasterisk.optdebug(spark).localize(query, faulty_where="total < 0")
for op in result.ranked:
    print(op)
```

```
1.000  [1] Aggregate branch — (amount > 1000)  (failing=1, passing=0)
0.500  [1] Aggregate — cid, sum(CASE WHEN ...) AS total GROUP BY cid  (failing=4, passing=8)
```

The planted fault negates amounts over 1000, which only one outlier record ever hits.
OptDebug puts that branch first with a perfect score, and leaves the aggregation — which
every record flows through — at a neutral 0.5.

The oracle is a `Row => Boolean` in Scala and a **SQL predicate** in Python, because a
predicate crosses a process boundary and a closure does not. Both forms are available
from Scala and agree.

## What gets scored

Two granularities, because operators alone are not enough:

- **The operator**, scored by which source records reach its output. This discriminates
  when the operator *drops* records — a join, a filter, a grouping.
- **Its branches**, scored by which source records take each arm of a `Filter`
  condition, an `IF`, or a `CASE WHEN`. This discriminates when the operator does not:
  every record flows through a projection, but only some take a given arm, and that is
  where a faulty operation inside an expression shows up.

Source scans are not scored — every record reaches them, so they carry no signal. A
join's condition is not offered as a branch, since which rows survive the join already
says the same thing.

## Narrowing the failing records first

Provenance says which records reached the faulty output. It cannot say which ones
*mattered*. For a grouped query that gap is large: every record of a faulty group is a
witness, including innocent rows that merely shared a key with the culprit — so every
operation the group flows through looks equally implicated.

Pass a base table to narrow the failing population by delta debugging before scoring. A
subset "still fails" if re-running the query with that table restricted to it still
produces a row the oracle rejects.

```scala
OptDebug.localize(spark, "orders", query, oracle)     // minimising form
```

```python
result = bigasterisk.optdebug(spark).localize(query, faulty_where="total < 0",
                                              base_table="orders")
result.minimised_from   # 4 — what provenance returned
result.failing_witnesses  # 1 — what actually causes the failure
```

This needs the query as **text**, not a DataFrame: minimisation re-runs it with the
table restricted, and a DataFrame's plan is already bound to the original relation. It
costs one query re-execution per subset tested, which is why it is opt-in.

## Choosing a formula

`Tarantula` is the default. It compares an operation's *rate* of failing coverage
against its rate of passing coverage, so an operation touching every record scores a
neutral 0.5 and one touching only failing records scores 1.0.

That neutrality is why it is the default **without narrowing**. `Ochiai` rewards raw
failing coverage, so an aggregation — which reaches every witness of a wrong result —
out-ranks the branch only the culprit took.

Narrowing changes that. With one failing witness instead of four, `Ochiai` gives the
faulty branch 1.0 and the aggregation 0.33, and is the better choice:

```scala
OptDebug.localize(spark, "orders", query, oracle, Suspiciousness.Ochiai)
```

The suite asserts both halves of this: that `Ochiai` picks the wrong operation without
narrowing, and the right one with it.

## Limitations

- **Narrowing needs a single named base table.** A query joining several tables can only
  be minimised over one of them at a time.
- **Narrowing re-runs the query** once per subset delta debugging tests. Fine for
  debugging, wrong for a hot path.
- **Scala UDF internals are opaque.** The paper propagates taint *inside* user-defined
  functions by source-to-source transformation. For a **Python** UDF that is available:
  register the function with [`bigasterisk.udf`](udfs.md) and its internal branches are
  bound to the columns the call site passes, then scored as operations in their own
  right — a faulty branch inside the function is ranked, not just the operator that
  calls it. A Scala UDF arrives as a closure whose logic is bytecode, and a fault inside
  one is still localised only to the operator that calls it.
- **Cost.** Every operation and branch is executed as its own provenance-captured
  sub-query. That is fine for debugging and wrong for a hot path.
- **Records are matched by content.** Lineage ids are positions assigned per execution,
  so they cannot be compared across the separate sub-queries this runs; witnesses are
  matched on the source columns both sides expose. Genuinely duplicated source rows
  therefore conflate.

## Where it lives

`modules/optdebug` reaches Spark only through `bigasterisk-api`. The scoring is
arithmetic, the provenance arrives through the binding, and delta debugging is a pure
algorithm in the shared API (`org.bigasterisk.api.DeltaDebug`) — so the module carries no
dependency on a Spark version. That is the shape any version-independent tool in this
repository should take.
