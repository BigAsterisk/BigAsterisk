# BigTest and NaturalSym — generating tests from the query itself

Symbolic test generation reads a query's own conditions, solves them, and constructs one
input per path through them. Rather than searching for an input that happens to reach a
branch, it builds a record that takes it.

- **BigTest** — *White-Box Testing of Big Data Analytics with Complex User-Defined
  Functions* (ESEC/FSE 2019)
- **NaturalSym** — *Natural Symbolic Execution-based Testing for Big Data Analytics*
  (FSE 2024)

The two differ in one thing: where the witness comes from. BigTest solves for *any*
value satisfying the path; NaturalSym prefers one that looks like real data. That is the
`natural` flag here, on by default.

## Using it

```scala
import org.bigasterisk.api.BigAsterisk

val suite = BigAsterisk.testgen(spark).generate(
  "SELECT cid FROM orders WHERE amount > 100",
  Map("orders" -> orders))

println(suite)                  // TestSuite(2 cases, 2 verified, 1 branches)
suite.cases.foreach(println)
```

```python
import bigasterisk

suite = bigasterisk.testgen(spark).generate(query, {"orders": orders})
for case in suite.cases:
    print(case)
```

```
[ok] (amount > 100)  (verified)
    orders: [o1,c1,420]
[ok] NOT (amount > 100)  (verified)
    orders: [o3,c3,80]
```

## Every test is executed

A generator that reports coverage it did not achieve is worse than useless. Every
generated input is run, and the branch it was built for is checked against what the
query actually did. `verified` says what happened; `note` says why when it did not.

That is also why an unsolvable path is reported rather than papered over:

```
[--] (amount > 200) AND (amount < 100)  (unsatisfiable)
[--] (amount > LENGTH(oid) * 100)       (condition outside the solver's fragment)
```

## The solver

Full symbolic execution is not needed for SQL predicates. A conjunction of comparisons
against literals is a set of interval and equality constraints per column, and a witness
can be read straight off the bounds. The solver understands:

| Form | Example |
|---|---|
| ordering | `amount > 100`, `amount <= 500` |
| equality | `cid = 'c2'`, `cid <> 'c9'` |
| nullness | `amount IS NULL`, `amount IS NOT NULL` |
| strings | `startswith(oid, 'o')`, `contains(oid, '1')` |
| conjunction | any `AND` of the above, and negations of them |

Integral columns step by one, so `amount > 100` yields `101` — the boundary, which is
where bugs live. Contradictions are detected rather than producing a bogus witness.

Anything outside that fragment — a constraint relating two columns, arithmetic on the
left-hand side, a disjunction that must hold — makes the path **unsupported**. The path
is reported as such rather than solved partially, because generating an input that
ignores half the constraint would be claiming coverage that was never achieved.

## Naturalness

A witness comes from one of three sources, in order of how much each knows about the
data:

1. a **declared distribution** for that column, if you gave one;
2. a value **observed** in the seed data, when `natural = true`;
3. a value **synthesised** from the solved bounds.

Each is used only if it satisfies the path, so naturalness never costs coverage.

### Declared distributions

You know the shape of your own data; the solver does not, and left to itself will
satisfy `age > 18` with `19` every time. Declare the shape and it will not:

```scala
TestGenConfig(distributions = Map(
  "name"   -> """Discrete("alice", "bob", "carol")""",
  "score"  -> "binom(100, 0.1)",
  "height" -> "normal(170, 10)",
  "amount" -> "uniform(0, 500)",
  "visits" -> "poisson(3)",
  "cid"    -> "zipf(1000, 1.2)"))
```

```python
bigasterisk.testgen(spark).generate(query, seeds, distributions={
    "score": "binom(100, 0.1)",
    "name": 'Discrete("alice", "bob")'})
```

`Discrete`, `uniform`, `normal`, `binom`, `poisson` and `zipf` are understood, case
insensitively. `zipf` is worth knowing about: a skewed key with a long tail is what makes
a generated join or grouping behave like a real one.

A declaration that cannot reach a path — `binom(100, 0.1)` can never exceed 100, so it
cannot satisfy `amount > 100` — falls back to a value that can, after a bounded number of
draws. Coverage wins over naturalness, and the suite pins that.

A misspelled declaration is rejected by name rather than silently ignored, since the
quiet failure mode would be tests that stop looking like the data without anyone
noticing.

### Observed values

With `natural = true` and no declaration, a witness is taken from values that actually
occur in the seed data whenever one satisfies the constraints. Same paths, same coverage;
the records read like records.

```scala
// natural: a real order that happens to satisfy the constraint
[ok] (amount > 100)  orders: [o1,c1,420]

// not natural: the boundary the solver derived
[ok] (amount > 100)  orders: [Ptl9Zm,K3sxQa,101]
```

## Paths versus branches

Every combination of branch outcomes is enumerated when there are few enough to fit
`maxPaths`. Beyond that, the suite falls back to taking and not taking each condition on
its own — branch coverage rather than path coverage. The distinction is reported rather
than hidden.

## Limitations

- **No SMT solver, so no path outside the fragment above.** The original BigTest drives
  a customized Java PathFinder and cvc5 over the *bytecode* of user-defined functions.
  Nothing here goes inside a UDF: the granularity is a SQL predicate.
- **Single-table constraints.** A constraint relating columns of two tables — a join
  condition — is not solved for.
- **Spark Connect.** Branch conditions are read from the driver-side analyzed plan,
  which a Connect client does not hold. Classic sessions only.

## Why this is not the original artifact

BigTest and NaturalSym are the two tools in this collection that could not be ported.
Both depend on a customized Java PathFinder / Symbolic PathFinder fork pinned to **JDK 8**
in deep ways: it ships modeled JDK 8 internal classes, reads bytecode up to class version
52, and relies on `sun.misc` APIs removed in JDK 9+. They also depend on `jad`, a
decompiler last released in 2001, and on linux/amd64 native binaries.

That machinery exists to reach *inside a Scala UDF*. Under a SQL front end there is no
UDF bytecode to symbolically execute — the conditions are in the plan, in a form Catalyst
already hands over. What is implemented here is the technique applied to that surface.
See
[PROVENANCE.md](https://github.com/BigAsterisk/BigAsterisk/blob/main/PROVENANCE.md).
