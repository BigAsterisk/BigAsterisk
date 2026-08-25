# Provenance of the migrated code

BigAsterisk is a migration, not a rewrite from scratch. This file records, for every
tool, exactly which upstream repository and commit the code in this repository derives
from, so any claim made here can be checked against the original artifact.

Commits are pinned at the revision that was read during migration. Upstream
repositories remain the historical record; they are not modified by this project.

## Status legend

- **integrated** — builds, runs, and is covered by tests in this repository
- **planned** — artifact gathered and analysed; migration not yet done
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
| BigDebug | planned | [maligulzar/bigdebug](https://github.com/maligulzar/bigdebug) | `2.1` | `b6baa11aff6d` | 2019-10-11 |
| FlowDebug | planned | [UCLA-SEAL/FlowDebug](https://github.com/UCLA-SEAL/FlowDebug) | `main` | `0ef74c7afd69` | 2022-06-03 |
| OptDebug | planned | [maligulzar/OptDebug](https://github.com/maligulzar/OptDebug) | `master` | `207a92b306e9` | 2021-10-25 |
| PerfDebug | planned | [UCLA-SEAL/PerfDebug](https://github.com/UCLA-SEAL/PerfDebug) | `main` | `ec6f93861fcc` | 2021-09-26 |
| DeSQL | integrated (reimplemented) | [SEED-VT/DeSQL](https://github.com/SEED-VT/DeSQL) | `Artifacts-default-branch` | `6855f746fcdb` | 2024-05-31 |
| Vega | **no artifact** | — | — | — | — |
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

### BigDebug — planned

The debugging primitives (`org.apache.spark.bdd`, roughly 2,600 lines) were never part
of the earlier migration. They are the one component that cannot be ported mechanically:
the original implements simulated breakpoints and on-demand watchpoints through a
**forked executor backend** (`BDExecutorBackend`, `BDDriverBackend`) that intercepts task
execution inside Spark. Reproducing the behaviour without forking Spark requires
re-implementing it on supported extension points. This is a re-architecture, and it is
tracked as such rather than as a port.

### FlowDebug and OptDebug — planned

Both already follow the attach-as-a-library approach that BigAsterisk standardises on:
they wrap `SparkContext` and `RDD` rather than patching Spark, so no fork is involved.
They also share roughly 90% of their source — the `provenance`, `symbolicprimitives` and
`sparkwrapper` packages are near-identical copies, with OptDebug adding operation-level
taint (`optdebug/OptDebug.scala`, `OptSetProvenance`, `BitSetProvenance`). They will be
migrated together onto one shared taint-provenance core rather than as two independent
codebases. The work is a Scala 2.11 → 2.13 and Spark 2.x → 4.x update.

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

### Vega — no artifact

No public source survives. The repository referenced by the paper does not exist, and
none of the 35 branches of `maligulzar/bigdebug` contains a Vega implementation; the
only remaining traces are two interface stubs in the FlowDebug benchmarks
(`TestingVega.scala`, `DDNonExhaustiveVega.scala`), which describe the shape of the
test-oracle API but not the incremental re-execution engine.

Vega will therefore be reimplemented from *Optimizing Interactive Development of
Data-Intensive Applications* (SoCC 2016), using the paper's own benchmarks and reported
speedups as the acceptance criterion. Any deviation from the published design will be
documented here and in the module's own documentation.

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
