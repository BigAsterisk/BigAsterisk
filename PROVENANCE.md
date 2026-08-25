# Provenance of the migrated code

BigAsterisk is a migration, not a rewrite from scratch. This file records, for every
tool, exactly which upstream repository and commit the code in this repository derives
from, so any claim made here can be checked against the original artifact.

Commits are pinned at the revision that was read during migration. Upstream
repositories remain the historical record; they are not modified by this project.

## Status legend

- **integrated** — builds, runs, and is covered by tests in this repository
- **planned** — artifact gathered and analysed; migration not yet done
- **partial** — some of the paper's primitives are implemented and tested; the tool's
  documentation page says which, and why the rest are outstanding
- **integrated (reimplemented)** — the technique is implemented and tested here, but
  the upstream source could not be ported because it depended on a forked Spark; the
  upstream artifact was used as the specification and cross-check
- **no artifact** — no public source survives; the technique must be reimplemented
  from the paper

## Upstream sources

| Tool | Status | Upstream repository | Branch | Commit | Upstream date |
|---|---|---|---|---|---|
| Titian | integrated | [SEED-VT/titian-spark-provenance](https://github.com/SEED-VT/titian-spark-provenance) | `main` | `7ea88d40a360` | 2026-06-19 |
| BigSift | integrated | [SEED-VT/titian-spark-provenance](https://github.com/SEED-VT/titian-spark-provenance) | `main` | `7ea88d40a360` | 2026-06-19 |
| BigDebug | integrated (reimplemented) | [maligulzar/bigdebug](https://github.com/maligulzar/bigdebug) | `2.1` | `b6baa11aff6d` | 2019-10-11 |
| FlowDebug | partial (reimplemented) | [UCLA-SEAL/FlowDebug](https://github.com/UCLA-SEAL/FlowDebug) | `main` | `0ef74c7afd69` | 2022-06-03 |
| OptDebug | partial (reimplemented) | [maligulzar/OptDebug](https://github.com/maligulzar/OptDebug) | `master` | `207a92b306e9` | 2021-10-25 |
| PerfDebug | integrated (reimplemented) | [UCLA-SEAL/PerfDebug](https://github.com/UCLA-SEAL/PerfDebug) | `main` | `ec6f93861fcc` | 2021-09-26 |
| DeSQL | integrated (reimplemented) | [SEED-VT/DeSQL](https://github.com/SEED-VT/DeSQL) | `Artifacts-default-branch` | `6855f746fcdb` | 2024-05-31 |
| Vega | integrated (reimplemented) | **no artifact** | — | — | — |
| BigTest | partial (reimplemented) | [SEED-VT/BigTest](https://github.com/SEED-VT/BigTest) | `master` | `5ce2cb968bb5` | 2026-06-17 |
| BigFuzz | integrated (reimplemented) | [UCLA-SEAL/BigFuzz](https://github.com/UCLA-SEAL/BigFuzz) | `main` | `b5d3deedd66a` | 2021-09-26 |
| DepFuzz | integrated (reimplemented) | [SEED-VT/DepFuzz](https://github.com/SEED-VT/DepFuzz) | `main` | `27bc8c509371` | 2026-06-15 |
| NaturalFuzz | integrated (reimplemented) | [SEED-VT/NaturalFuzz](https://github.com/SEED-VT/NaturalFuzz) | `main` | `77ad7ffaa761` | 2025-05-04 |
| NaturalSym | integrated (reimplemented) | [UCLA-SEAL/NaturalSym](https://github.com/UCLA-SEAL/NaturalSym) | `main` | `e7924fd3e3a9` | 2025-02-15 |

### Secondary and historical sources

These were consulted during migration but are not the primary source for any module:

| Repository | Commit | Relevance |
|---|---|---|
| [UCLA-SEAL/Titian](https://github.com/UCLA-SEAL/Titian) | `146ef598bbf2` | Original Titian, as a Spark 1.6/2.1 fork |
| [UCLA-SEAL/BigDebug](https://github.com/UCLA-SEAL/BigDebug) | `146ef598bbf2` | Same fork as UCLA-SEAL/Titian |
| [UCLA-SEAL/BigSift](https://github.com/UCLA-SEAL/BigSift) | `59cde688bcde` | Zeppelin/Docker demo harness only; no engine source |
| [maligulzar/BigSift-Zeppelin](https://github.com/maligulzar/BigSift-Zeppelin) | `403e32c0198e` | The BigSift demo's cluster environment |
| [UCLA-SEAL/OptDebug](https://github.com/UCLA-SEAL/OptDebug) | `37d140344108` | Pointer to the Virginia Tech repository; no source |
| [UCLA-SEAL/BigTest](https://github.com/UCLA-SEAL/BigTest) | `06644d78c8d4` | Original BigTest artifact |

## Per-tool migration notes

### Titian and BigSift — integrated

The upstream repository is itself a completed migration of Titian and BigSift from the
Spark 1.2/2.1 fork to an attach-as-a-library for stock Spark 4.1.x, extended with Spark
SQL / DataFrame provenance that the 2016 original did not have. BigAsterisk imports it
as `modules/spark4` (the capture engine) and `modules/bigsift` (the isolation
algorithm), and adds the `modules/api` binding layer in front of both.

Deviations from the published Titian design are documented in
[docs/developer-guide.md](docs/developer-guide.md). The most significant is that SQL
capture is implemented with codegen-fused tap operators, a mechanism that did not exist
in the paper because the paper predates whole-stage codegen being a capture surface.

### BigDebug — reimplemented, all primitives

The debugging primitives (`org.apache.spark.bdd`, roughly 2,600 lines) were never part
of the earlier migration, and they cannot be ported mechanically: the original works
through a **forked executor backend** (`BDExecutorBackend`, `BDDriverBackend`) that
intercepts task execution inside Spark.

**All of the paper's primitives are implemented**, for Spark SQL and PySpark. Three
mechanisms in the original each needed the fork, and each has a stock-Spark equivalent:

| Original | Needed a fork because | Replacement |
|---|---|---|
| Predicate shipped as bytecode, hot-loaded via `AbstractFileClassLoader` (`WatchpointManager.writePredicateClass`) | class files written to `/tmp` on each executor and loaded out of band | the guard is a Catalyst expression, which Spark already serializes with the plan |
| Matches streamed by `SendWatchpointDataToDriver`, added to `CoarseGrainedClusterMessages` | a new message in Spark's RPC protocol | `AccumulatorV2`, Spark's own executor-to-driver channel |
| Capture attached by a patched task iterator (`BDIterator`, `WatchPointLRDD`) | task execution intercepted inside Spark | a `SparkPlan` operator injected through `spark.sql.extensions`, fused into whole-stage codegen |

Deliberate differences from the upstream implementation:

- **Bounded capture.** The original sent every matching record to the driver. Here
  matches are counted in full but only `capacity` rows are retained, so a guard that
  matches a billion rows reports its true selectivity without moving a billion rows.
- **Column pruning is suppressed through the watchpoint**, so captured rows keep the
  schema of the DataFrame that was watched rather than whatever the rest of the query
  happened to need.
- **No live predicate replacement.** The original could swap a watchpoint's predicate on
  a running job by pushing new bytecode. Here a new guard means a new watchpoint.

Crash-culprit determination rests on one further detail. Accumulators are merged only
from tasks that *succeed*, which is exactly wrong when the interesting task is the one
that died; Spark supports merging updates from failed tasks through `countFailedValues`
on registration, which is how its own task metrics survive a failure. That flag is what
makes the primitive possible without the forked executor backend. Its limit is a batched
Python or Arrow UDF above the guard: the batch crosses to the worker before any of it can
fail, so the guard has already emitted the whole batch. That cannot be detected from the
guard — unlike PerfDebug, whose risk is a batched operator *below* the profiling point
and therefore visible in the plan it holds.

Fine-grained latency alerts are covered by PerfDebug.

**Simulated breakpoints** needed no mechanism at all, once the paper's design was read
carefully. A simulated breakpoint does not pause the computation: it "retains information
to re-generate the program state from the latest materialization point", so that setting
one has "almost zero overhead". The state at a point in a query is defined by the plan up
to that point, and that plan is already in hand — so regenerating the state is a matter
of executing it, and setting a breakpoint costs nothing until someone asks. No operator
is inserted, and the suite asserts the executed plan is unchanged by one.

### FlowDebug — partial: influence-based provenance implemented

The paper has two halves. Where each stands:

| Contribution | Status |
|---|---|
| Influence-based provenance for many-to-one dependencies: rank a result's inputs by how much each contributed, from the aggregate's semantics | **implemented for SQL** |
| Fine-grained taint inside user-defined functions, inserted by source-to-source transformation | **implemented for Python UDFs**, by reading the function's source rather than rewriting it; Scala UDFs remain opaque |

The influence half maps directly onto SQL, because a SQL aggregate's semantics are
known in advance: only the largest record influences a `MAX`, and a record's influence
on a `SUM` is the magnitude of its contribution. No taint and no re-execution are
needed — the ranking is read off the values entering the aggregation in a single pass.
For a `MAX` over a group of n records, provenance returns n and influence returns 1,
which is the precision improvement the paper reports.

The taint half is not ported but re-derived, and the mechanism is necessarily
different. The upstream implementation rewrites the user's Scala program
(`refactor/ProvenanceInserter.scala`, `symbolicprimitives/`) to carry taint-bearing
values through it; a SQL query is not a program to rewrite. What *is* a program is a
Python UDF, and its source is readable from the front end, so taint is computed by
static analysis of it instead — which parameters reach a returned value, and which
decide a branch whose arms return different things. The result is that an influential
record's *columns* are narrowed to the ones that could actually reach the result
(`Influence.columns`), which is the precision the taint half exists to provide.

Two differences from the original are worth stating. It is *static*, so it reports what
can influence rather than what did on a particular row; and it covers Python only,
because a Scala UDF's logic arrives on the JVM as bytecode. Anything the analysis cannot
read is reported, and the whole call stays implicated.

Note that the upstream FlowDebug and OptDebug share roughly 90% of their source — the
`provenance`, `symbolicprimitives` and `sparkwrapper` packages are near-identical
copies. Neither was ported: both were re-derived for SQL, which is why no shared
taint-provenance core appears here.

Deliberate differences from the upstream implementation:

- **Aggregations only.** A query with no many-to-one dependency is reported as such,
  since provenance is already exact for it.
- **The group is collected to the driver.** Practical for explaining one suspicious
  result, not for scoring every group of a large table.
- **The topmost aggregation is the one analysed** when a query nests several.

### OptDebug — partial, reimplemented for SQL

The upstream artifact is an RDD-level tool: it wraps `SparkContext`, propagates
operation taint through `symbolicprimitives`, and rewrites user code by source-to-source
transformation (`refactor/ProvenanceInserter.scala`). None of that applies to a SQL
front end, where there is no user Scala program to rewrite. The technique was therefore
re-derived for Spark SQL.

The paper rests on three insights. Where each stands:

| Insight | Status |
|---|---|
| Use provenance to shrink the input to a small failing/passing set before debugging | **implemented** — opt-in, by delta debugging over a named base table |
| Track operation provenance, so it is known which operations processed which records | **implemented for SQL**, at the granularity of plan operators and their conditional branches — including branches inside a Python UDF, bound to the columns the call site passes |
| Rank operations by spectra — participation in failing versus passing outcomes | **implemented** (Tarantula and Ochiai) |

Deliberate differences from the upstream implementation:

- **Granularity.** The original propagates taint inside user-defined functions. Here the
  unit is a conditional expression — a `Filter` condition, an arm of an `IF` or
  `CASE WHEN` — and, for a Python UDF with a registered profile, a branch inside the
  function, bound to the columns the call site passes and scored as an operation in its
  own right. A fault inside a *Scala* UDF is still localised only to the operator that
  calls it.
- **How spectra are gathered.** No instrumentation and no taint-carrying values. Each
  operation is executed as its own provenance-captured sub-query, and its spectrum is
  the intersection of the records reaching it with the failing and passing populations.
- **Tarantula is the default**, not Ochiai, because narrowing is opt-in. Without it the
  failing population contains innocent records that merely share a group with the
  culprit, and Ochiai's reward for raw failing coverage then ranks the query's
  aggregation — which every record reaches — above the branch only the culprit took.
  Tarantula scores such an operation a neutral 0.5. With narrowing on, Ochiai becomes
  the better choice, and the suite asserts the reversal in both directions.
- **Narrowing is over one named base table**, and needs the query as text rather than a
  DataFrame, since it re-runs the query with that table restricted.
- **Witnesses are matched by content, not by lineage id.** Ids are positions assigned
  per execution, so an id in one sub-query is unrelated to the same record's id in
  another. Matching uses the source columns both sides expose, which means genuinely
  duplicated source rows conflate.

### PerfDebug — partial, reimplemented for SQL

The upstream artifact propagates a latency value alongside every record through the
forked Spark's lineage machinery (`lineage/perfdebug/perftrace`, `PerfTraceCalculator`)
and stores the results in **Apache Ignite** (`lineage/perfdebug/ignite`). Both were
consequences of needing to carry per-record state across stage boundaries in a Spark of
that era, and neither survives here: the timing is taken inside Spark's generated code
and travels back by `AccumulatorV2`, with no external store.

What is implemented is **per-record cost at a profiling point you choose**: the clock is
read once per record inside the generated loop, and the interval between consecutive
records is the work the upstream pipeline did for the later one. Totals are exact; only
the `topK` most expensive records are materialised.

Deliberate differences from the upstream implementation:

- **Measured at a point, attributed on demand.** The original carries a latency value
  alongside every record through every stage, so the total is always available. Here you
  place a profiling point, and attributing a particular expensive result back to the
  inputs responsible is done at the point of asking, by tracing that result's provenance
  and matching it against what was measured. Same question answered, without carrying
  per-record state across stage boundaries — which is the part that needed the fork.
- **No Ignite, no external store.** Retained records live in the accumulator.
- **Record-level attribution stops at a batched operator.** A Python or Arrow UDF
  computes a whole batch in one call to another process, so the batch's cost cannot be
  pinned to the record that caused it. `PerfProfile.recordLevel` reports this at runtime
  rather than leaving the caller to infer it; totals can also understate in that case,
  because the batch's cost falls in the interval before a task's first record, which is
  never retained.

Two implementation details were found by testing rather than assumed, and both are
pinned by the suite: upstream expressions must be forced before the clock is read (Spark
emits an input variable's code at its first use, so a costly UDF would otherwise be
charged to the *next* record), and the first record of each task must be excluded (its
interval spans pipeline start-up).

### DeSQL — integrated, reimplemented

Upstream is a fork of Spark 3.0 (`spark-sql-debug`), with the tool confined to
`sql/core/.../sql/debugger/SubQueryStorage.scala` and a Spark UI tab. It could not be
ported, because it does not stand on Catalyst's public surface: it calls
`plan.getMappingIndex()`, `plan.allChildren`, `plan.accept(visitor)` and
`spark.getDebugBuffer()`, none of which exist in Apache Spark. The fork adds them —
a `mappingIndex` field and visitor hooks injected into Catalyst's own plan and
expression classes, plus `SubQueryGeneratorVisitor` and `DataRegeneratorVisitor` under
`org.apache.spark.sdb`.

BigAsterisk therefore re-derives the same decomposition from the **unmodified analyzed
plan**. Attributes in a Catalyst plan flow strictly bottom-up, so the subtree rooted at
any node is already a complete, resolved query computing "the query so far"; wrapping
each node with `Dataset.ofRows` yields that step's intermediate data. This needs no
injected field and no fork.

Deliberate differences from the upstream implementation:

- **Steps come from the analyzed plan**, so they follow the order the query states
  rather than the order Spark will execute after optimization.
- **Wrapper nodes are folded away** (`SubqueryAlias`, `View`) and their names carried
  down, so a scan reports itself as `orders AS o` rather than as an anonymous relation.
- **No Spark UI tab.** Upstream renders results into a forked UI page; the
  reimplementation exposes steps as ordinary `DataFrame`s, so any front end — notebook,
  shell, PySpark — can display them.
- **Correlated subqueries are refused** with a clear error rather than returning rows,
  since their plans carry outer references and cannot execute standalone.

### Vega — reimplemented from the paper

No public source survives. The repository referenced by the paper does not exist, and
none of the 35 branches of `maligulzar/bigdebug` contains a Vega implementation; the
only remaining traces are two interface stubs in the FlowDebug benchmarks
(`TestingVega.scala`, `DDNonExhaustiveVega.scala`), which describe the shape of the
test-oracle API but not the incremental re-execution engine.

Vega was therefore written from *Optimizing Interactive Development of Data-Intensive
Applications* (SoCC 2016). The paper describes two optimizations:

| Optimization | Status |
|---|---|
| Reuse materialized intermediate results from the previous run of a similar program | **implemented** |
| Rewrite the dataflow to push code modifications as late as possible, so execution can start from a later materialization point | **implemented for filters** |

The first is implemented by decomposing a query into its parts (the same decomposition
DeSQL exposes), materializing the reusable ones, and matching a later revision against
them on Catalyst's `canonicalized` plan form. That is the same basis Spark's own
`CacheManager` uses, so materializing a part is sufficient for the optimizer to route a
later revision through it — no plan substitution is needed.

The second is implemented for **filters**: a predicate written below a join — inside a
derived table or a CTE, where analysis really does place it beneath the join — is pulled
above it, so the join stays identical to the one the previous revision materialized. It
is applied as a normalisation to every revision, since a revision can only reuse what the
previous one stored, and only to queries containing a join, since without expensive work
to get past there is nothing to gain.

The rewrite is legal only where it cannot change the answer: through an inner join on
either side, an outer join on the preserved side, a projection that carries the filter's
columns through unchanged, and an alias — never through an aggregation. Catalyst pushes
filters back down while optimising, so the rewritten query plans to the same physical
plan; the cost is that the materialized join is the unfiltered one, which is larger, and
that is precisely what makes it survive an edit to the filter.

An edit to a projection or an aggregate is not relocated.

**No performance claim is made.** The paper reports up to three orders of magnitude on
its own benchmarks; this implementation has not been run against them. Doing so requires
reconstructing the benchmark programs, which are not part of any surviving artifact
either.

### BigTest and NaturalSym — partial, reimplemented for SQL predicates

These are the two tools that could not be ported. Both depend on a customized Java
PathFinder / Symbolic PathFinder fork pinned to **JDK 8** in deep ways: it ships modeled
JDK 8 internal classes, reads bytecode up to class version 52, and relies on `sun.misc`
APIs removed in JDK 9+. They also depend on `jad`, a decompiler last released in 2001,
and on linux/amd64 native binaries. The upstream BigTest repository documents the
constraint and a staged path out of it, budgeting the JPF port as weeks of work.

That machinery exists for one purpose: to reach *inside a UDF*. Under a SQL front end
most conditions never go there — they are in the plan, in a form Catalyst already hands
over — so the technique was applied to that surface first. For the conditions that *are*
inside a UDF, a Python function's source is readable from the front end, and reading it
reaches the same place without a bytecode symbolic executor: see
**Reading inside a Python UDF** below. Scala UDF bytecode still needs JPF.

| Contribution | Status |
|---|---|
| Enumerate paths through the dataflow's conditions and solve for an input per path | **implemented for SQL predicates** |
| Symbolically execute the *bytecode* of user-defined functions (JPF/SPF + cvc5) | **implemented for Python UDFs** by static analysis of the function's source — paths, path constraints and return values, so a condition on a UDF's result becomes conditions on its arguments. Scala UDF bytecode is **not implemented** |
| NaturalSym: prefer witnesses that look like real data | **implemented**, as "a value observed in the seed data" |
| NaturalSym: sample from user-supplied input distributions | **implemented** |

The solver is an interval-and-equality domain per column rather than an SMT solver,
which is sufficient for conjunctions of SQL comparisons against literals and is honest
about its limits: a constraint relating two columns, arithmetic on the left-hand side,
or a disjunction that must hold makes the path *unsupported* and it is reported as such,
never solved partially.

Deliberate differences from the upstream implementations:

- **Every generated test is executed and checked.** The suite reports whether the input
  actually took the path it was built for, rather than asserting coverage from the
  solver's say-so.
- **Integral bounds step by one**, so `amount > 100` yields `101` — the boundary, which
  is where bugs live.
- **Paths degrade to branches under a budget.** Every combination is enumerated when it
  fits `maxPaths`; beyond that each condition is taken and not taken on its own, and the
  distinction is reported rather than hidden.
- **Declarations are per column name, not per positional input.** NaturalSym's input
  annotations are one line per input table, each column separated by `|`. Under a SQL
  front end columns have names, so declarations are keyed by name — the same information,
  addressed the way SQL addresses it. `Discrete`, `uniform`, `normal`, `binom`, `poisson`
  and `zipf` are understood; a declaration that cannot reach a path falls back to a value
  that can, so naturalness never costs coverage.

### BigFuzz, DepFuzz, NaturalFuzz — one fuzzer, three strategies

The three papers differ in exactly one decision: where a generated value comes from.
They are therefore one fuzzer here with three mutation strategies, rather than three
codebases sharing 90% of their source — which is what the upstream artifacts are.

| Paper | Strategy | Status |
|---|---|---|
| NaturalFuzz | path vectors over the query's branch predicates | **implemented** |
| NaturalFuzz | corpus minimisation to a bounded sample per distinct path vector | **implemented** |
| NaturalFuzz | `natural` — interleaving: splice the deciding columns of a row that reaches an uncovered branch into another real row | **implemented** |
| DepFuzz | branch-level influence: which columns of which dataset decide each branch | **implemented** |
| DepFuzz | `co-dependent` — join equalities repaired jointly across the datasets they tie together | **implemented** |
| BigFuzz | `random` — values drawn for the column's type, plus a boundary set | **implemented** |
| BigFuzz | framework abstraction: run the dataflow's semantics without Spark | **implemented** |

Coverage targets are the query's conditional branches, and each conjunct of a compound
condition is profiled separately — `a AND b` is one condition to the query but two
decisions to a fuzzer, usually decided by different columns. An input reaching a new
branch is kept and mutated further, which is the guidance both papers describe.

**Where the branch analysis comes from.** Both papers obtain it by taint analysis:
instrumenting each dataflow operator *and each branch predicate*, then tracking
`(dataset, column, row)` tags through user code. Under a SQL front end that is not an
approximation — a predicate's attributes carry expression ids, a leaf relation's output
carries the same ids, so the map from a branch to the columns that decide it, and from a
join to the columns it ties together, reads exactly off the analyzed plan
(`BranchProfiler`).

Both claims are reproduced and asserted by the suite:

- *NaturalFuzz.* On `WHERE amount > 90000 AND cid = 'c1'`, where no seed row satisfies
  both conjuncts, interleaving covers all four branch targets and drawing values covers
  two — reaching the conjunction requires one row's `amount` with another's `cid`.
- *DepFuzz.* On a joined query, mutation that ignores co-dependence produces empty
  results far more often, because a freely mutated join key essentially never matches.

Deliberate differences from the upstream implementations:

- **Framework abstraction covers a subset of SQL.** Each iteration interprets the
  query's analyzed plan over in-memory rows using Catalyst's own expression evaluation
  and declarative aggregates, so the operator semantics are Spark's while the planning,
  scheduling and task setup are gone. Scans, projections, filters, inner joins,
  `UNION ALL`, `LIMIT`, `DISTINCT` and grouped or global `SUM`/`COUNT`/`MAX`/`MIN`/`AVG`
  are supported; anything else falls back to Spark rather than being approximated. On a
  joined, filtered, grouped query over twenty rows this measured **69.8x** — 68.4 ms per
  iteration through Spark against 1.0 ms interpreted. The suite pins that both paths
  produce the same answer; the ratio itself is a benchmark
  (`examples/runMain org.bigasterisk.examples.FuzzAbstractionBenchmark`) rather than a
  test, since it depends on the machine.
- **Generation is schema-driven, not per-benchmark.** The upstream artifacts hand-write
  a mutation operator per benchmark program (`GenCommuteTypeData`, `GenFlightData`, and
  so on). Under a SQL front end the input is a table with a schema, so the schema drives
  generation and no per-benchmark code exists.
- **Influence is plan-level, so it stops at a UDF.** Which columns decide a `WHERE` or a
  `CASE` is exact, and exact is stronger than the upstream taint analysis, which is
  necessarily conservative. What plan analysis cannot see is a branch *inside* a UDF —
  the same bytecode boundary FlowDebug and BigTest reach. Such an operator is profiled as
  though it had no branches.
- **Co-dependence covers join equalities.** A join on a range or an inequality is left to
  the mutation strategy rather than repaired.
- **Failures are exceptions, not oracle violations.** A campaign finds inputs that make
  the query throw; checking that an answer is *correct* is BigSift's and OptDebug's job.

## Reading inside a Python UDF

Three of the tools here stop at the same boundary, and all three stop at it for the same
reason: a user-defined function is opaque to plan analysis. FlowDebug needs taint inside
one, OptDebug needs operation-level taint inside one, BigTest needs path constraints from
one. Upstream, each crosses it by analysing code — source-to-source rewriting for the
first two, JPF/SPF over bytecode for the third.

For a **Python** UDF, the code is Python source and the front end can read it directly.
`python/bigasterisk/udf.py` parses the function and records what it found;
`org.bigasterisk.api.UdfRegistry` holds the result; `org.apache.spark.sql.udf.UdfAnalysis`
binds it back to the query by substituting each parameter for the argument expression the
call site passes, then resolving the result with Spark's own analyzer.

| Analysis | What it yields | Which tool uses it |
|---|---|---|
| Branch extraction | each `if` condition, as a predicate over the call site's columns | operation isolation (scored as an operation), test generation (a coverage target) |
| Path enumeration | the conjunction of branch outcomes reaching each `return`, and what it returns | test generation — a condition on the function's *result* becomes conditions on its *arguments* |
| Taint | which parameters reach a returned value, or decide a branch whose arms return different things | influence-based provenance — narrowing an influential record to the columns that could reach the result |

Differences from the upstream analyses, stated plainly:

- **Python only.** A Scala or Java UDF arrives on the JVM as a closure whose logic is
  bytecode. Those are still black boxes, and each consumer says so rather than guessing.
- **Row-at-a-time UDFs only.** A pandas UDF's parameters are Series rather than values,
  so the same source means something different. Refused by eval type.
- **Static, not dynamic.** Taint here reports what *can* influence the result, not what
  did on a particular row. It is sound for exclusion — a parameter absent from the set
  cannot change the answer — which is the direction that matters for narrowing.
- **A readable subset, and it refuses the rest out loud.** Comparisons, boolean
  operators, null tests, membership, `+ - * /`, a handful of string and numeric
  functions, conditional expressions in a `return`, locals, and free variables that
  resolve to constants. Python truthiness (`if amount:`) and the operators Spark and
  Python disagree about on negative operands (`%`, `//`, `**`) are deliberately refused:
  a subtly wrong branch condition would mis-rank an operation or generate a test that
  proves nothing. Whatever could not be read is listed in the profile, the affected
  paths are marked inexact, and an inexact path is never solved through.
- **Keyed by name, and explicit.** A plan carries a Python UDF's name, so that is what
  the two sides agree on; a profile whose parameter count does not match the call is
  never applied. Nothing is registered by default, so a query behaves exactly as it did
  before until a profile is registered for it.

## Reproducing the artifact survey

The upstream commits above were read from public repositories. To re-fetch any of them:

```bash
git clone --filter=blob:none https://github.com/<owner>/<repo>.git
git -C <repo> checkout <commit>
```
