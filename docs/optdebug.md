# OptDebug — which operation is at fault

Data provenance answers *which records* produced a wrong result. It says nothing about
*which part of the query* was at fault. OptDebug closes that gap: it scores a query's
operations the way spectrum-based fault localisation scores lines of code. An operation
that most failing records passed through, and few passing records did, is the one to
look at.

From *OptDebug: Fault-Inducing Operation Isolation for Dataflow Applications*
(SoCC 2021).

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

## Choosing a formula

`Tarantula` is the default. It compares an operation's *rate* of failing coverage
against its rate of passing coverage, so an operation touching every record scores a
neutral 0.5 and one touching only failing records scores 1.0.

That neutrality is the reason it is the default here. `Ochiai` rewards raw failing
coverage, so a query's aggregation — which reaches every witness of a wrong result — out-ranks
the branch only the culprit took. `Ochiai` is available and is the better choice once
the failing population has been narrowed to the records actually responsible.

```scala
OptDebug.localize(spark, df, oracle, Suspiciousness.Ochiai)
```

## Limitations

- **Input minimisation is not implemented.** The paper's first insight is to shrink the
  input with [BigSift](bigsift.md) before computing spectra, so the failing population
  contains only records that actually cause the failure. Here the failing population is
  every source record behind a rejected output, which for a grouped query includes
  innocent members of the same group. This is why `Tarantula` is the default; wiring
  BigSift in would sharpen the ranking and make `Ochiai` viable.
- **UDF internals are opaque.** The paper propagates taint *inside* user-defined
  functions by source-to-source transformation. Here the finest granularity is a
  conditional expression in the SQL plan, so a fault inside a Scala or Python UDF is
  localised to the operator that calls it, not to a line within it.
- **Cost.** Every operation and branch is executed as its own provenance-captured
  sub-query. That is fine for debugging and wrong for a hot path.
- **Records are matched by content.** Lineage ids are positions assigned per execution,
  so they cannot be compared across the separate sub-queries this runs; witnesses are
  matched on the source columns both sides expose. Genuinely duplicated source rows
  therefore conflate.

## Where it lives

`modules/optdebug` reaches Spark only through `bigasterisk-api`. The scoring is
arithmetic and the provenance arrives through the binding, so the module carries no
dependency on a Spark version — the same shape any version-independent tool in this
repository should take.
