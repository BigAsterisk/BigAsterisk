# FlowDebug — which records actually mattered

Ordinary provenance answers a yes/no question: did this record contribute to that
result? For a many-to-one dependency the answer is nearly useless. Every record of a
group contributed to that group's aggregate, so tracing a wrong `MAX` over a
million-row group hands you a million records.

FlowDebug asks *how much* each contributed. Only the largest record influences a `MAX`.
A record's influence on a `SUM` is the size of its contribution. That turns a million
candidates into the handful worth looking at.

From *Influence-Based Provenance for Dataflow Applications with Taint Propagation*
(SoCC 2020).

!!! info "What is implemented"
    **Influence-based provenance** is implemented, for Spark SQL and PySpark. The
    paper's other half — propagating taint *inside* user-defined functions — is
    **not**; see [below](#taint-inside-udfs).

## Using it

```scala
import org.bigasterisk.api.BigAsterisk

val df = spark.sql("SELECT cid, MAX(amount) AS peak FROM orders GROUP BY cid")
BigAsterisk.influence(spark).influencers(df, "peak > 1000").foreach(println)
```

```python
import bigasterisk

ranked = bigasterisk.influence(spark).influencers(
    "SELECT cid, MAX(amount) AS peak FROM orders GROUP BY cid",
    faulty_where="peak > 1000")
print(ranked[0])
```

```
1.0000  [o8,c2,99999]  (only the maximum influences)
0.0000  [o2,c2,250]    (below the maximum; no influence)
0.0000  [o5,c2,190]    (below the maximum; no influence)
0.0000  [o11,c2,205]   (below the maximum; no influence)
```

Provenance would return all four records. Influence names the one that decided the
answer, and says why.

The predicate names the results to explain, in terms of the **aggregation's** output
columns. Scores within a group sum to 1, so they read as shares of responsibility.

## The rules

| Aggregate | Influence |
|---|---|
| `MAX` | 1 for the largest record, 0 for the rest; ties share it |
| `MIN` | 1 for the smallest record, 0 for the rest; ties share it |
| `SUM`, `AVG` | proportional to the magnitude of the contribution |
| `COUNT` | equal for every record |
| anything else | equal for every record, and the reason says so |

A record is as influential as the most influential thing it does: when a query
aggregates several ways, a record that decides one of them matters, whatever it did to
the others.

Nulls contribute nothing to a `SUM`. When every contribution to a `SUM` is zero, records
are weighted equally rather than divided by zero.

A query with **no** aggregation is reported as such — provenance is already exact there,
because every record maps to at most one result.

## How it is computed

One pass over the rows entering the aggregation, carrying the grouping key and each
aggregate's argument as extra columns. Everything after that is arithmetic on collected
rows. There is no taint propagation and no re-execution: the semantics of `MAX` are
known in advance, so influence can be read off the values rather than measured by
removing records and looking at what changes.

## Taint inside UDFs

The paper's other contribution is fine-grained tracking of control and data flow *within*
user-defined functions, achieved by rewriting the user's program to carry custom data
abstractions.

For a **Python** UDF that is available here, by reading the function's own source. The
result is a narrower answer: which record influenced a result is half of it, and which
of that record's *columns* did is the other half.

```python
def score(amount, note):
    return amount * 2          # `note` is passed and never read

bigasterisk.udf.register(spark, score)

ranked = bigasterisk.influence(spark).influencers(
    "SELECT cid, MAX(score(amount, oid)) AS peak FROM orders GROUP BY cid",
    faulty_where="peak > 1000")

ranked[0].columns     # {'amount'} — without the analysis, both arguments are implicated
ranked[0].narrowed    # True
```

An argument the function never lets reach its return — or one that only decides a branch
whose arms return the same thing — provably cannot change the result, whatever the call
site looks like. Anything the analysis cannot read is reported and the whole call stays
implicated, which is the over-approximation this is meant to avoid but is still the
honest answer. See [Python UDFs](udfs.md) for what is read and what is refused.

A Scala UDF arrives on the JVM as a closure whose logic is bytecode. Those remain
opaque, and every column the call reads stays implicated.

## Limitations

- **Aggregations only.** Influence is defined for many-to-one dependencies. A query
  without one gets a uniform ranking and a note saying why.
- **The group is collected to the driver.** Practical for debugging one suspicious
  result; not for scoring every group of a large table at once.
- **Nested aggregations.** The topmost aggregation in the plan is the one analysed.
- **Spark Connect.** The aggregation is located in the driver-side analyzed plan, which
  a Connect client does not hold. Classic sessions only.
