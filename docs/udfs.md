# Seeing inside a UDF

A user-defined function is opaque to plan analysis. To Catalyst,

```sql
SELECT oid FROM orders WHERE classify(amount) = 'high'
```

is one call, over one column, producing one value. There are no branches to score, no
constraints to solve, and every argument looks equally responsible for the result. Three
techniques here stop at exactly that boundary — and the papers behind them cross it by
reading the function's code.

Both Python and Scala UDFs are readable here, by different routes — because the code
lives in different places.

```python
import bigasterisk

def classify(amount):
    if amount > 1000:
        return "high"
    elif amount > 100:
        return "medium"
    return "low"

spark.udf.register("classify", classify, StringType())
bigasterisk.udf.register(spark, classify)
```

## Python: parse the source

`register` parses the function and records what it found:

```
UdfProfile(classify(amount), 2 branches, 3 paths, solvable)
```

Nothing else changes about how you write queries. From here on the engines know the
function has two branches over `amount`, three paths, that `amount` influences the
result, and — because every path is exact — that `classify(amount) = 'high'` means
`amount > 1000`.

## Scala and Java: read the bytecode

Nothing to register. A Scala UDF's closure is serializable — Spark requires it — so its
`writeReplace` yields a `SerializedLambda` naming the class and method the body was
compiled into. That class is on the classloader, so the bytecode loads, and the method is
abstractly interpreted over a symbolic stack: parameters are symbols, constants are
values, and a conditional jump forks the analysis.

```scala
val classify = udf((amount: Int) => if (amount > 1000) "high" else "low")
spark.udf.register("classify", classify)
// nothing else to do
```

The result is the same profile a Python UDF produces, so everything downstream treats
them identically. Conditions read as the source wrote them: the opposite arm of a branch
inverts the operator rather than wrapping the text in `NOT`, so `amount > 1000` and
`amount <= 1000` are one branch and its negation rather than two unrelated strings.

A captured value — a threshold held in a local — is folded in as the constant it is:

```scala
val threshold = 500
val over = udf((amount: Int) => if (amount > threshold) "over" else "under")
// the branch reads (arg0 > 500)
```

## What it changes

=== "Test generation"

    A condition on a UDF's *result* cannot be solved: no solver inverts an opaque call.
    With a profile, the conditions under which the function returns that result are
    ordinary comparisons on its arguments, and those can be solved.

    ```python
    suite = bigasterisk.testgen(spark).generate(
        "SELECT oid FROM orders WHERE classify(amount) = 'high'", {"orders": orders})
    ```

    Without a profile every case comes back `condition outside the solver's fragment`.
    With one, the suite contains an input built for each path — `amount > 1000`,
    `NOT amount > 1000 AND amount > 100`, and neither — each executed and verified.

=== "Operation isolation"

    A branch inside a UDF is invisible to the plan, so an operation-level ranking cannot
    name it. Bound to the column the call site passes, it becomes an ordinary condition
    and is scored like any other operation.

    ```python
    faulty = spark.sql("SELECT oid, band(amount) AS value FROM orders")
    result = bigasterisk.optdebug(spark).localize(faulty, "value < 0")
    result.ranked[0]        # the branch (amount > 1000), inside band
    ```

=== "Influence-based provenance"

    Which record mattered is half the answer; which of its *columns* mattered is the
    other half. An argument the function never lets reach its return cannot change the
    result, however prominently it appears in the call.

    ```python
    ranked = bigasterisk.influence(spark).influencers(
        "SELECT cid, MAX(score(amount, oid)) AS peak FROM orders GROUP BY cid",
        faulty_where="peak > 1000")

    ranked[0].columns       # {'amount'} — `oid` is passed and never read
    ranked[0].narrowed      # True
    ```

## What it reads

**Python.** `if` / `elif` / `else` over comparisons of parameters against literals;
`and`, `or`, `not`; `is None`; `in` over a literal collection, and `in` as a substring
test; arithmetic with `+ - * /`; `len`, `abs`, `.upper()`, `.lower()`, `.strip()`,
`.startswith()`, `.endswith()`; a conditional expression in a `return`; assignments to
locals; and free variables — module-level constants and closed-over values — that
resolve to scalars.

**Scala and Java.** Comparisons of an argument against a constant, integer and
floating-point arithmetic, `equals`, `startsWith`, `endsWith`, `contains`, `length`,
`toUpperCase`, `toLowerCase`, `trim`, `Math.abs`, null tests, boxing and unboxing, and
the branch structure `&&`, `||` and `!` compile into. Values the closure captured are
folded in as constants.

A path through the function is the conjunction of the branch outcomes leading to one
`return`, including the implicit `return None` when control falls off the end.

## What it refuses

Everything else, out loud. The profile lists what it could not read, the paths affected
are marked inexact, and every consumer falls back to treating the function as the black
box it was:

```python
profile = bigasterisk.udf.analyze(some_function)
profile.complete       # False
profile.unsupported    # ["condition 's.encode('utf8') == b'x'': method 'encode' is not understood"]
profile.solvable       # False — an inexact path is never solved through
```

Two refusals are worth naming, because both look like they would work:

- **Python truthiness.** `if amount:` is true for any non-zero number and any non-empty
  string. SQL has no such coercion, so rendering it as a predicate would change its
  meaning. Write the comparison out.
- **`%`, `//` and `**`.** Spark and Python disagree about these on negative operands. A
  constraint that is subtly wrong is worse than one that is missing.

A wrong branch condition would mis-rank an operation or generate a test that proves
nothing, which is worse than no analysis at all. Nothing is assumed.

## Limitations

- **Loops and exception handlers are refused** by the bytecode analysis, along with any
  call it does not model. A refusal is recorded and the affected paths are marked
  inexact, exactly as on the Python side.
- **A Scala UDF's parameters have no names.** Bytecode carries argument *positions*, not
  the names the source used, so a profile derived from bytecode calls them `arg0`,
  `arg1`. They bind to the call site by position, which is what matters; only the
  reported text differs.
- **Row-at-a-time UDFs only.** A pandas UDF's parameters are Series, not values, so the
  same source means something different. They are left alone.
- **A registered profile wins over a derived one.** If you register a profile for a name
  a Scala UDF also uses, the registered one is used: someone who described the function
  explicitly knows more about it than its bytecode reveals.
- **Profiles are keyed by name.** A plan carries a Python UDF's name, so a name is what
  the two sides can agree on. Two different functions registered under one name are
  indistinguishable, and the later registration wins — register under the name the query
  uses. A profile whose parameter count does not match the call is never applied.
- **The source has to exist.** A function defined in a REPL or built by `exec()` has no
  source for `inspect.getsource` to return, and `analyze` says so rather than guessing.
- **Registration is per driver, and explicit.** The registry is empty until you fill it,
  so a query behaves exactly as it did before until you register something. That is also
  what makes this safe to adopt piecemeal.

## Inspecting a profile

For a Python UDF, `analyze` does everything `register` does except talk to the JVM, so a
profile can be examined — or tested — without a Spark session:

```python
profile = bigasterisk.udf.analyze(classify)

profile.parameters     # ['amount']
profile.branches       # [('amount > 1000', {'amount'}), ('amount > 100', {'amount'})]
profile.paths          # [('amount > 1000', "'high'", True), ...]
profile.influencing    # {'amount'}
profile.lines()        # the wire format the JVM registry reads
```

`bigasterisk.udf.registered(spark)` lists the names currently profiled, and
`bigasterisk.udf.unregister(spark, name)` forgets one.
