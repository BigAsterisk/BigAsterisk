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

## What the fuzzer knows about the query

Before generating anything, the campaign works out **which columns of which table decide
each branch**. Both splicing strategies stand on this: mutate the columns that decide a
branch and the query's control flow moves; mutate the rest and nothing happens.

The published tools obtain this by taint analysis — instrumenting each branch predicate
and tracking `(dataset, column, row)` tags through the program. Under a SQL front end it
is not an approximation at all: a predicate's attributes carry expression ids, a leaf
relation's output carries the same ids, and the mapping reads straight off the analyzed
plan.

Two things fall out of it.

**Path vectors.** Every seed row is evaluated against every branch, giving it one bit
per branch — `1` where it satisfies the branch, `0` where it does not, `-` where the
branch belongs to another table and this row has no say. That bit string is the row's
path vector.

```
orders                     amount > 90000   cid = 'c1'   both
o1  c1  420                      0              1          0     ->  "010"
o8  c2  99999                    1              0          0     ->  "100"
```

Two rows with the same vector are interchangeable as far as the query's control flow is
concerned, so the corpus is **minimised** to a bounded sample of each distinct vector —
`rowsPerVector`, three by default. A hundred rows that all take the same path through the
query are worth no more to a fuzzer than two of them, and cost fifty times as much to
search.

**Co-dependence.** A join equality makes two datasets dependent: mutate one side freely
and no row survives, the query returns nothing, and the iteration teaches the campaign
nothing. Equalities are resolved through expression ids, so a join between `orders.cid`
and `customers.cid` is recognised as one constraint, while two unrelated columns that
merely share a name are not.

## The three strategies

| Strategy | How a row is built | Paper |
|---|---|---|
| `random` | every value drawn for the column's type, from nothing | BigFuzz |
| `natural` | a real row, with the deciding columns spliced in from another real row | NaturalFuzz |
| `co-dependent` | as `natural`, then join equalities repaired across tables | DepFuzz |

**`random`** is the baseline: cheap, and good at finding crashes on malformed values.
It is poor at anything behind a join, because a randomly generated key essentially never
matches one on the other side — and poor at categorical branches, because an invented
string essentially never equals `'c1'`.

**`natural`** builds a candidate by **interleaving**: take a row from the corpus, pick a
branch nothing has covered yet, find a donor row whose path vector satisfies that branch,
and copy the donor's *deciding columns* into the base row. Every value in the result is
one that genuinely occurred in that column — the right formats, plausible magnitudes —
while the combination is new, and aimed at coverage rather than at random.

This is what a per-column mutator cannot do. On

```sql
SELECT oid FROM orders WHERE amount > 90000 AND cid = 'c1'
```

no seed row satisfies both conjuncts: the one large order belongs to `c2`, and `c1`'s
orders are small. Reaching the conjunction means combining one row's `amount` with
another's `cid`. Each conjunct is profiled separately for exactly this reason — `a AND b`
is one condition to the query but two decisions to a fuzzer, usually decided by different
columns. The suite pins the outcome: with the same budget, splicing covers all four
branch targets of that query and drawing values covers two.

**`co-dependent`** is the default. It splices as `natural` does, then **repairs** every
join equality the plan declares: a shared value is written into each table the equality
ties together, so the generated tables still join. Mutation stays joint across
co-dependent regions instead of drifting the two sides apart.

The difference is measurable, and the suite asserts it: on a joined query, `random`
produces empty results far more often than `co-dependent` does.

Whatever the strategy, one candidate in ten is nudged onto a **boundary value** — zero,
negative one, `Int.MaxValue`, the empty string, `NaN`, a 256-character string. Plausible
data alone will not find the crash on an empty string or an overflowing sum. With ANSI
mode on, this is what finds an integer overflow in `amount + amount`.

## Running without Spark

A campaign runs the same small query thousands of times. Run through Spark, almost all
of that is framework — planning, scheduling, task serialization, shuffle setup — work
that dwarfs the query itself when the input is twenty rows.

By default each iteration is evaluated by **interpreting the query's analyzed plan over
in-memory rows** instead. The operator semantics are Spark's: expressions are evaluated
with Catalyst's own interpreted evaluation, and aggregates run Spark's own declarative
definitions. What is gone is the framework around them, and the re-planning per
iteration — the plan is analyzed once and the generated rows are substituted at its
leaves.

On a joined, filtered, grouped query over twenty rows:

```
50 iterations of the same campaign:
  through Spark     3421 ms   (  68.4 ms per iteration)
  abstracted          49 ms   (   1.0 ms per iteration)
  speedup             69.8x
```

Reproduce it with
`bin/sbt 'examples/runMain org.bigasterisk.examples.FuzzAbstractionBenchmark'`. The ratio
depends on the machine, which is why it is a benchmark rather than a test — what the
suite pins is that **both paths produce the same answer**, for the same seed.

### What it supports, and what it refuses

Scans, projections, filters, inner joins, `UNION ALL`, `LIMIT`, `DISTINCT`, and grouped
or global aggregation with `SUM`, `COUNT`, `MAX`, `MIN` and `AVG`.

Anything else — an outer join, `ORDER BY`, a `DISTINCT` aggregate — is **refused**, and
that iteration runs on Spark instead. A faster oracle that quietly disagrees with the
real one is worse than no oracle, so the interpreter never approximates: every supported
shape is checked differentially against Spark, and every unsupported one is checked to
say so rather than guess.

Set `abstractFramework = false` (`abstract_framework=False` in Python) to run everything
through Spark. `FuzzResult.abstracted` reports how many iterations avoided it.

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

- **The interpreter covers a subset.** Queries outside it fall back to Spark, which is
  correct but slow. Widening the subset widens the speedup.
- **Failures are exceptions, not oracles.** A campaign finds inputs that make the query
  *throw*. It does not check that a query returns the right answer.
- **Branch influence is plan-level.** Which columns decide a `WHERE` or a `CASE` is
  exact. Which columns decide a branch *inside a UDF* is not visible without bytecode
  analysis, so a query whose control flow hides in user code is profiled as though that
  operator had no branches.
- **Equalities only.** Co-dependence is repaired for join equalities. A join on a range
  or an inequality is left to the mutation strategy.
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
