# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[semantic versioning](https://semver.org/spec/v2.0.0.html).

Because this project migrates published research systems, entries say what was
reproduced from a paper and what was not. `PROVENANCE.md` holds the full account.

## [Unreleased]

### Added

- **The platform**, on a version-pluggable binding. Everything Spark-version-specific
  sits behind the `SparkBinding` service interface; tools compile against
  `modules/api` alone, so supporting a future Spark release means adding a binding
  module rather than changing any tool.
- **Titian** — record-level data provenance, backward and forward, for SQL, PySpark and
  the RDD API.
- **BigSift** — the minimal fault-inducing input records, by provenance and delta
  debugging.
- **DeSQL** — step-through debugging of a SQL query by decomposing it into its parts,
  re-derived from the unmodified analyzed plan.
- **BigDebug** — simulated breakpoints, on-demand watchpoints, and crash-culprit
  determination.
- **Vega** — incremental re-execution across query revisions, including the rewrite that
  moves an edit later so the work beneath it stays reusable.
- **OptDebug** — ranking the operations behind a wrong result, over operators and the
  branches of their conditions, with the failing records narrowed first.
- **PerfDebug** — per-record cost at a chosen point, and attribution of a particular
  expensive result to the inputs behind it.
- **FlowDebug** — influence-based provenance, ranking an aggregate's inputs by how much
  each actually mattered.
- **BigFuzz, DepFuzz, NaturalFuzz** — one fuzzer with three mutation strategies, and
  framework abstraction: iterations are interpreted over in-memory rows rather than run
  as Spark jobs.
- **BigTest, NaturalSym** — systematic test generation from a query's own conditions,
  with declared input distributions.
- A **notebook for each tool**, each ending in assertions and executed in CI.
- A **platform tour** (`bin/bigasterisk tour`) running every tool in one process.
- A **self-contained toolchain** (`bin/bootstrap`): JDK, sbt, Python and Spark into
  `tools/`, so a build never depends on the host.

### Not reproduced

Three gaps remain, and they are the same boundary seen from three sides: reaching
*inside* a user-defined function. FlowDebug's taint propagation, OptDebug's
operation-level taint, and BigTest's symbolic execution of UDF bytecode all need
bytecode analysis rather than plan analysis. Each tool's page says so.
