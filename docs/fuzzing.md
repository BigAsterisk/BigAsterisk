# Fuzzing — BigFuzz, NaturalFuzz and DepFuzz

Testing a data-intensive application normally means running it over a real dataset:
slow, and it covers only the cases that dataset happens to contain. Fuzzing generates
small inputs instead, and steers them toward the parts of a query nothing has exercised
yet.

Three papers, one loop. BigFuzz, NaturalFuzz and DepFuzz differ in exactly one
decision — where a generated value comes from — so they are one fuzzer here with three
mutation strategies, rather than three codebases that share 90% of their source.

- **BigFuzz** — *Efficient Fuzz Testing for Data Analytics Using Framework Abstraction*
  (ASE 2020)
- **DepFuzz** — *Co-dependence Aware Fuzzing for Dataflow-Based Big Data Analytics*
  (ESEC/FSE 2023)
- **NaturalFuzz** — *Natural Input Generation for Big Data Analytics* (ASE 2023)

## Using it

```scala
import org.bigasterisk.api.{BigAsterisk, FuzzConfig}

val result = BigAsterisk.fuzz(spark).fuzz(
  """SELECT c.name, SUM(o.amount) AS total
    |FROM orders o JOIN customers c ON o.cid = c.cid
    |WHERE o.amount > 100 GROUP BY c.name""".stripMargin,
  Map("orders" -> orders, "customers" -> customers),
  FuzzConfig(iterations = 100))

println(result)                       // coverage and failure counts
result.failures.foreach(println)      // the inputs that broke it
```

```python
import bigasterisk

result = bigasterisk.fuzz(spark).fuzz(
    query, {"orders": orders, "customers": customers}, iterations=100)

print(result.coverage)
for failure in result.failures:
    print(failure)
```

`seeds` maps each table the query reads to a DataFrame. Its rows are the corpus that
generated values are drawn from. The campaign swaps generated data in under those table
names while it runs, and **restores the originals afterwards**.

## The three strategies

| Strategy | Where a value comes from | Paper |
|---|---|---|
| `random` | drawn at random for the column's type | BigFuzz |
| `natural` | spliced column-wise out of rows already seen | NaturalFuzz |
| `co-dependent` | as `natural`, but joined columns share one pool | DepFuzz |

**`random`** is the baseline: cheap, and good at finding crashes on malformed values.
It is poor at anything behind a join, because a randomly generated key essentially never
matches one on the other side.

**`natural`** splices values column-wise from observed data, so every value is one that
genuinely occurred in that column — the right formats, plausible magnitudes — while the
*combinations* are new. Generated rows look like data rather than like noise.

**`co-dependent`** is the default. A join makes two columns of two tables dependent:
mutate one freely and the rows stop matching, the query returns nothing, and the campaign
learns nothing. Join equalities are read out of the analyzed plan, and the columns they
tie together draw from a shared pool.

The difference is measurable, and the suite asserts it: on a joined query, `random`
produces empty results far more often than `co-dependent` does.

Whatever the strategy, one value in ten comes from a **boundary set** — zero, negative
one, `Int.MaxValue`, the empty string, `NaN`, a 256-character string. Plausible-looking
data alone will not find the crash on an empty string or an overflowing sum. With ANSI
mode on, this is what finds an integer overflow in `amount + amount`.

## Coverage and guidance

The coverage targets are the query's **branches**: a `Filter` condition and its
negation, each arm of a `CASE WHEN`.

An input that reaches a branch nothing had reached is kept in the corpus and mutated
further, which is what makes a campaign a search rather than a sampler. So is an input
that makes the query fail. Set `guided = false` to draw every candidate from the seed
data instead.

Branches are evaluated in **one aggregation per distinct input plan**, not one query per
branch, so the per-iteration cost stays close to the cost of running the query itself.

A campaign is reproducible: same `seed`, same result.

## Limitations

- **Framework abstraction is not implemented.** BigFuzz's headline result — removing
  98% of the setup overhead by running the dataflow's semantics without Spark — is not
  reproduced. Every iteration here runs a real Spark job, so a campaign is orders of
  magnitude slower per iteration than the paper's. What is reproduced is the mutation
  and guidance, not the framework abstraction.
- **Failures are exceptions, not oracles.** A campaign finds inputs that make the query
  *throw*. It does not check that a query returns the right answer.
- **Co-dependence is matched by column name.** A join between `orders.cid` and
  `customers.cid` pools both, which is right. Two unrelated columns that happen to share
  a name and appear in some join condition would also pool, which is conservative rather
  than wrong — it widens the pool, it does not break the join.
- **Spark Connect.** Branch coverage is read from the driver-side analyzed plan, which a
  Connect client does not hold. Classic sessions only.

## Relationship to the published tools

All three upstream artifacts are RDD-level Scala tools built on a shared
`abstraction`/`fuzzer`/`guidance` skeleton, with mutation operators written per
benchmark program (`GenCommuteTypeData`, `GenFlightData`, and so on). None of that
transfers to a SQL front end, where the input is a table with a schema rather than a
program with hand-written generators — so the schema drives generation here, and no
per-benchmark code is needed.

See
[PROVENANCE.md](https://github.com/BigAsterisk/BigAsterisk/blob/main/PROVENANCE.md) for
the upstream sources this was checked against.
