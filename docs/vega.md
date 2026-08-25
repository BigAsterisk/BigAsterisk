# Vega — incremental re-execution across query revisions

Exploratory analysis is a sequence of near-identical queries. You run one, look at the
answer, change a projection or add a grouping, and run it again. Each run normally
starts from nothing, even though most of the work is the same as last time.

Vega materializes the reusable parts of a query as it runs, so the next revision starts
from the deepest point the two still share.

From *Optimizing Interactive Development of Data-Intensive Applications* (SoCC 2016).

!!! warning "Reimplemented from the paper"
    No source for Vega survives — see [below](#no-artifact-survives). Both of the
    paper's optimizations are implemented, but nothing here has been benchmarked against
    its reported numbers.

## Using it

```scala
import org.bigasterisk.api.BigAsterisk

val vega = BigAsterisk.vega(spark)

val v1 = vega.run(spark.sql("SELECT cid, amount FROM orders WHERE amount > 100"))
v1.df.collect()

// a revision: the scan and the filter are unchanged, so their result is reused
val v2 = vega.run(spark.sql(
  "SELECT cid, SUM(amount) AS total FROM orders WHERE amount > 100 GROUP BY cid"))
v2.reused        // Seq("Filter — (amount > 100)")
v2.reuseRatio    // the share of this query's parts that came from v1
v2.df.collect()
```

```python
import bigasterisk

vega = bigasterisk.vega(spark)
vega.run("SELECT cid, amount FROM orders WHERE amount > 100").df.collect()

r = vega.run("SELECT cid, SUM(amount) AS total FROM orders "
             "WHERE amount > 100 GROUP BY cid")
print(r.reused, r.reuse_ratio)
r.df.collect()
```

`run` returns the DataFrame to execute. It is **semantically identical** to the one you
passed in — Vega changes how much work a query does, never what it returns.

## How reuse is decided

A query decomposes into parts, one per node of its plan. Each
part's plan is a complete sub-query, so its result can be materialized and handed to a
later revision that still contains it.

Matching is on Catalyst's `canonicalized` form, which normalizes attribute ids and other
incidental differences. Two revisions parsed separately therefore match on the parts
they genuinely share — you do not have to reuse the same DataFrame object, or even
phrase the shared part identically.

That is deliberately the same basis Spark's own `CacheManager` uses to decide whether a
cached plan applies. Materializing a part is therefore enough: the optimizer routes a
later revision through it without any plan surgery on our side.

## What is materialized

Materialization costs time and memory on the run that performs it, and pays for itself
on the next revision. That trade-off is the technique. Two kinds of part are skipped:

- **Source scans.** Re-reading a source is what the storage layer is for; caching one
  trades memory for no saving in work.
- **The final result.** A revision differs there by definition.

At most `maxMaterialized` parts (default 8) are held at once. The cap is deliberately
small: reuse across revisions comes overwhelmingly from the few parts nearest the
sources. `clear()` releases everything.

## Moving an edit later

Reuse is worth only as much as the prefix two revisions share, and an edit near the
sources destroys almost all of it. Change a filter written inside a derived table and
everything above it — including the join computed last time — would have to be
recomputed, even though the join itself did not change.

So the filter is moved above the join instead:

```
as written                          rewritten
Join                                Filter (amount > 200)
  Filter (amount > 200)               Join
    orders                              orders
  customers                           customers
```

The join below is now identical to the one the previous revision materialized, and the
edited filter applies to its result. `VegaRun.rewritten` reports when this happened.

This is a **normalisation**, applied to every revision rather than only where it pays
off — a revision can only reuse what the previous one stored, so both must be in the
same shape. It runs only on queries containing a join, since without expensive work to
get past there is nothing to gain.

It is legal only where it cannot change the answer: through an inner join on either
side, through an outer join on the preserved side only, through a projection that
carries the filter's columns through unchanged, and through an alias. Never through an
aggregation — filtering rows before grouping and filtering groups afterwards are
different queries.

Two things make this cheap. Catalyst pushes filters back down while optimising, so the
rewritten query plans to the same physical plan and costs nothing at execution time. And
a plain `WHERE` needs no help at all: an *analysed* plan already puts it above the join,
in the order the query states.

The cost is memory. The materialized join is the unfiltered one, which is larger — and
is exactly why it survives an edit to the filter.

## Limitations
- **Memory.** Materialized parts live in the executors for the session, at
  `MEMORY_AND_DISK`. Call `clear()` when you move on.
- **Spark Connect.** Reuse is decided against the driver-side analyzed plan, which a
  Connect client does not hold. Classic sessions only.
- **No performance claims.** The paper reports up to three orders of magnitude on its
  own benchmarks. This implementation has not been benchmarked against them, and no
  such claim is made for it here.
- **Only filters move.** The rewrite relocates predicates. An edit to a projection or an
  aggregate stays where it is.

## No artifact survives

Vega is the one tool in BigAsterisk with no surviving public source. The repository the
paper points at does not exist, and none of the 35 branches of `maligulzar/bigdebug`
contains an implementation; the only remaining traces are two interface stubs in the
FlowDebug benchmarks (`TestingVega.scala`, `DDNonExhaustiveVega.scala`), which describe
the shape of a test-oracle API but not the re-execution engine.

This implementation was therefore written from the paper. See
[PROVENANCE.md](https://github.com/BigAsterisk/BigAsterisk/blob/main/PROVENANCE.md) for
what was and was not reproduced.
