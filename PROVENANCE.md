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
| FlowDebug | planned | [UCLA-SEAL/FlowDebug](https://github.com/UCLA-SEAL/FlowDebug) | `main` | `0ef74c7afd69` | 2022-06-03 |
| OptDebug | partial (reimplemented) | [maligulzar/OptDebug](https://github.com/maligulzar/OptDebug) | `master` | `207a92b306e9` | 2021-10-25 |
| PerfDebug | planned | [UCLA-SEAL/PerfDebug](https://github.com/UCLA-SEAL/PerfDebug) | `main` | `ec6f93861fcc` | 2021-09-26 |
| DeSQL | integrated (reimplemented) | [SEED-VT/DeSQL](https://github.com/SEED-VT/DeSQL) | `Artifacts-default-branch` | `6855f746fcdb` | 2024-05-31 |
| Vega | partial (reimplemented) | **no artifact** | — | — | — |
| BigTest | planned | [SEED-VT/BigTest](https://github.com/SEED-VT/BigTest) | `master` | `5ce2cb968bb5` | 2026-06-17 |
| BigFuzz | planned | [UCLA-SEAL/BigFuzz](https://github.com/UCLA-SEAL/BigFuzz) | `main` | `b5d3deedd66a` | 2021-09-26 |
| DepFuzz | planned | [SEED-VT/DepFuzz](https://github.com/SEED-VT/DepFuzz) | `main` | `27bc8c509371` | 2026-06-15 |
| NaturalFuzz | planned | [SEED-VT/NaturalFuzz](https://github.com/SEED-VT/NaturalFuzz) | `main` | `77ad7ffaa761` | 2025-05-04 |
| NaturalSym | planned | [UCLA-SEAL/NaturalSym](https://github.com/UCLA-SEAL/NaturalSym) | `main` | `e7924fd3e3a9` | 2025-02-15 |

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

### FlowDebug — planned

FlowDebug already follows the attach-as-a-library approach BigAsterisk standardises on:
it wraps `SparkContext` and `RDD` rather than patching Spark, so no fork is involved.
The work is a Scala 2.11 → 2.13 and Spark 2.x → 4.x update, plus a decision about what
"taint inside a UDF" means when the front end is SQL rather than an RDD chain.

Note that FlowDebug and the upstream OptDebug share roughly 90% of their source — the
`provenance`, `symbolicprimitives` and `sparkwrapper` packages are near-identical
copies. Migrating FlowDebug should produce the shared taint-provenance core that a
fuller OptDebug would also use.

### OptDebug — partial, reimplemented for SQL

The upstream artifact is an RDD-level tool: it wraps `SparkContext`, propagates
operation taint through `symbolicprimitives`, and rewrites user code by source-to-source
transformation (`refactor/ProvenanceInserter.scala`). None of that applies to a SQL
front end, where there is no user Scala program to rewrite. The technique was therefore
re-derived for Spark SQL.

The paper rests on three insights. Where each stands:

| Insight | Status |
|---|---|
| Use provenance to shrink the input to a small failing/passing set before debugging | **not implemented** — the failing population is every source record behind a rejected output |
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
- **Tarantula is the default**, not Ochiai. Without the input minimisation above, the
  failing population contains innocent records that merely share a group with the
  culprit, and Ochiai's reward for raw failing coverage then ranks the query's
  aggregation — which every record reaches — above the branch only the culprit took.
  Tarantula scores such an operation a neutral 0.5.
- **Witnesses are matched by content, not by lineage id.** Ids are positions assigned
  per execution, so an id in one sub-query is unrelated to the same record's id in
  another. Matching uses the source columns both sides expose, which means genuinely
  duplicated source rows conflate.

### PerfDebug — planned

Builds on the same lineage capture as Titian, adding per-record latency propagation.
The upstream artifact stores lineage in Apache Ignite; the migration will target the
capture engine already in `modules/spark4` instead, which removes the external
dependency.

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

### BigTest and NaturalSym — planned

These are the hardest to modernise, and not because of Spark. Both depend on a
customized Java PathFinder / Symbolic PathFinder fork that is pinned to **JDK 8** in
deep ways: it ships modeled JDK 8 internal classes, reads bytecode up to class version
52, and relies on `sun.misc` APIs removed in JDK 9+. The upstream BigTest repository
documents this constraint and a staged path out of it.

BigTest never runs Spark — it decompiles benchmark bytecode and symbolically executes
the UDFs, so Spark is only a compile-time API for the programs under test. That makes
the symbolic engine separable from the Spark version, and it is why these two tools are
scheduled after the debugging tools.

### BigFuzz, DepFuzz, NaturalFuzz — planned

DepFuzz and NaturalFuzz share a common ancestor and duplicate their `abstraction`,
`fuzzer`, `guidance` and `provenance` packages; they differ mainly in the guidance
strategy. They will be migrated onto one fuzzing core with pluggable guidance. BigFuzz
is on a different stack (Java, Maven, JQF instrumentation) and is treated separately.

## Reproducing the artifact survey

The upstream commits above were read from public repositories. To re-fetch any of them:

```bash
git clone --filter=blob:none https://github.com/<owner>/<repo>.git
git -C <repo> checkout <commit>
```
