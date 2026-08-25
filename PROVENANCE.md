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
| BigDebug | partial (watchpoints) | [maligulzar/bigdebug](https://github.com/maligulzar/bigdebug) | `2.1` | `b6baa11aff6d` | 2019-10-11 |
| FlowDebug | partial (reimplemented) | [UCLA-SEAL/FlowDebug](https://github.com/UCLA-SEAL/FlowDebug) | `main` | `0ef74c7afd69` | 2022-06-03 |
| OptDebug | partial (reimplemented) | [maligulzar/OptDebug](https://github.com/maligulzar/OptDebug) | `master` | `207a92b306e9` | 2021-10-25 |
| PerfDebug | partial (reimplemented) | [UCLA-SEAL/PerfDebug](https://github.com/UCLA-SEAL/PerfDebug) | `main` | `ec6f93861fcc` | 2021-09-26 |
| DeSQL | integrated (reimplemented) | [SEED-VT/DeSQL](https://github.com/SEED-VT/DeSQL) | `Artifacts-default-branch` | `6855f746fcdb` | 2024-05-31 |
| Vega | partial (reimplemented) | **no artifact** | — | — | — |
| BigTest | partial (reimplemented) | [SEED-VT/BigTest](https://github.com/SEED-VT/BigTest) | `master` | `5ce2cb968bb5` | 2026-06-17 |
| BigFuzz | partial (reimplemented) | [UCLA-SEAL/BigFuzz](https://github.com/UCLA-SEAL/BigFuzz) | `main` | `b5d3deedd66a` | 2021-09-26 |
| DepFuzz | integrated (reimplemented) | [SEED-VT/DepFuzz](https://github.com/SEED-VT/DepFuzz) | `main` | `27bc8c509371` | 2026-06-15 |
| NaturalFuzz | integrated (reimplemented) | [SEED-VT/NaturalFuzz](https://github.com/SEED-VT/NaturalFuzz) | `main` | `77ad7ffaa761` | 2025-05-04 |
| NaturalSym | partial (reimplemented) | [UCLA-SEAL/NaturalSym](https://github.com/UCLA-SEAL/NaturalSym) | `main` | `e7924fd3e3a9` | 2025-02-15 |

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

### BigDebug — partial: watchpoints implemented

The debugging primitives (`org.apache.spark.bdd`, roughly 2,600 lines) were never part
of the earlier migration, and they cannot be ported mechanically: the original works
through a **forked executor backend** (`BDExecutorBackend`, `BDDriverBackend`) that
intercepts task execution inside Spark.

**On-demand watchpoints are implemented**, for Spark SQL and PySpark. Three mechanisms
in the original each needed the fork, and each has a stock-Spark equivalent:

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

Still outstanding, and tracked as re-architecture rather than as a port: **simulated
breakpoints**, **crash-culprit determination**, and **fine-grained latency alerts**.
The first two rest on the same task-level interception the forked executor backend
provided; the third overlaps with PerfDebug.

### FlowDebug — partial: influence-based provenance implemented

The paper has two halves. Where each stands:

| Contribution | Status |
|---|---|
| Influence-based provenance for many-to-one dependencies: rank a result's inputs by how much each contributed, from the aggregate's semantics | **implemented for SQL** |
| Fine-grained taint inside user-defined functions, inserted by source-to-source transformation | **not implemented** |

The influence half maps directly onto SQL, because a SQL aggregate's semantics are
known in advance: only the largest record influences a `MAX`, and a record's influence
on a `SUM` is the magnitude of its contribution. No taint and no re-execution are
needed — the ranking is read off the values entering the aggregation in a single pass.
For a `MAX` over a group of n records, provenance returns n and influence returns 1,
which is the precision improvement the paper reports.

The taint half has no counterpart under a SQL front end: the upstream implementation
rewrites the user's Scala program (`refactor/ProvenanceInserter.scala`,
`symbolicprimitives/`), and a SQL query is not a program to rewrite while a Python UDF
is opaque to the JVM. The nearest equivalent in this repository is OptDebug's branch
scoring, which distinguishes records by which arm of a `CASE WHEN` or `Filter` they
took — inside the query's own expressions, though not inside a UDF.

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
| Track operation provenance, so it is known which operations processed which records | **implemented for SQL**, at the granularity of plan operators and their conditional branches |
| Rank operations by spectra — participation in failing versus passing outcomes | **implemented** (Tarantula and Ochiai) |

Deliberate differences from the upstream implementation:

- **Granularity.** The original propagates taint inside user-defined functions. The
  finest granularity here is a conditional expression of the SQL plan — a `Filter`
  condition, an arm of an `IF` or `CASE WHEN`. A fault inside a Scala or Python UDF is
  localised to the operator that calls it, not to a line within it.
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

- **Measured at a point, not propagated through the pipeline.** The original computes a
  latency for every record at every stage and traces the total back to the inputs. Here
  you place a profiling point and get the cost of the pipeline below it. This is a
  smaller claim, and it is the part that does not need a fork.
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

### Vega — partial, reimplemented from the paper

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
| Rewrite the dataflow to push code modifications as late as possible, so execution can start from a later materialization point | **not implemented** |

The first is implemented by decomposing a query into its parts (the same decomposition
DeSQL exposes), materializing the reusable ones, and matching a later revision against
them on Catalyst's `canonicalized` plan form. That is the same basis Spark's own
`CacheManager` uses, so materializing a part is sufficient for the optimizer to route a
later revision through it — no plan substitution is needed.

Without the second optimization, an edit near the sources invalidates everything above
it, which is precisely the case the paper's rewrite exists to rescue. Expect reuse on
revisions that extend or change a query near its output, not on ones that change an
early filter.

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

That machinery exists for one purpose: to reach *inside a Scala UDF*. Under a SQL front
end there is no UDF bytecode to symbolically execute — the conditions are in the plan,
in a form Catalyst already hands over. The technique was therefore applied to that
surface instead.

| Contribution | Status |
|---|---|
| Enumerate paths through the dataflow's conditions and solve for an input per path | **implemented for SQL predicates** |
| Symbolically execute the *bytecode* of user-defined functions (JPF/SPF + cvc5) | **not implemented** |
| NaturalSym: prefer witnesses that look like real data | **implemented**, as "a value observed in the seed data" |
| NaturalSym: sample from user-supplied input distributions | **not implemented** |

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
- **Naturalness is "a value that occurred", not a distribution.** NaturalSym's input
  annotations (`input1 := Discrete("alice","bob") | scipy.binom(100, 0.1)`) have no
  equivalent here.

### BigFuzz, DepFuzz, NaturalFuzz — one fuzzer, three strategies

The three papers differ in exactly one decision: where a generated value comes from.
They are therefore one fuzzer here with three mutation strategies, rather than three
codebases sharing 90% of their source — which is what the upstream artifacts are.

| Paper | Strategy | Status |
|---|---|---|
| NaturalFuzz | `natural` — splice values column-wise out of observed rows | **implemented** |
| DepFuzz | `co-dependent` — joined columns draw from a shared pool, so rows survive the join | **implemented** |
| BigFuzz | `random` — values drawn for the column's type, plus a boundary set | **implemented** |
| BigFuzz | framework abstraction: run the dataflow's semantics without Spark | **not implemented** |

Coverage targets are the query's conditional branches — the same ones DeSQL exposes and
OptDebug scores — and an input reaching a new branch is kept and mutated further, which
is the guidance the DepFuzz and NaturalFuzz papers describe.

The DepFuzz claim is reproduced and asserted by the suite: on a joined query, random
mutation produces empty results far more often than co-dependent mutation, because a
randomly generated join key essentially never matches.

Deliberate differences from the upstream implementations:

- **No framework abstraction.** BigFuzz's headline result is removing ~98% of setup
  overhead by executing the dataflow's semantics outside Spark. Every iteration here
  runs a real Spark job, so per-iteration cost is orders of magnitude higher than the
  paper's. The mutation and guidance are reproduced; the abstraction is not.
- **Generation is schema-driven, not per-benchmark.** The upstream artifacts hand-write
  a mutation operator per benchmark program (`GenCommuteTypeData`, `GenFlightData`, and
  so on). Under a SQL front end the input is a table with a schema, so the schema drives
  generation and no per-benchmark code exists.
- **Co-dependence is matched by column name** rather than by tracking which code segment
  reads which dataset region. Join equalities come straight out of the analyzed plan.
- **Failures are exceptions, not oracle violations.** A campaign finds inputs that make
  the query throw; checking that an answer is *correct* is BigSift's and OptDebug's job.

## Reproducing the artifact survey

The upstream commits above were read from public repositories. To re-fetch any of them:

```bash
git clone --filter=blob:none https://github.com/<owner>/<repo>.git
git -C <repo> checkout <commit>
```
