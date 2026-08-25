# BigAsterisk

**A unified debugging and testing platform for Apache Spark.**

BigAsterisk brings thirteen research systems for debugging and testing data-intensive
scalable computing — built between 2016 and 2024 at UCLA and Virginia Tech — into a
single maintained codebase that runs on **stock Apache Spark**.

Each of these tools originally shipped as its own fork of Spark, pinned to the release
it was written against. That is why almost none of them can be run today. BigAsterisk
replaces the forks with one library that attaches to an unmodified Spark distribution
through a versioned binding layer, so the techniques outlive the release they were
written for.

## Where to go

<div class="grid cards" markdown>

- :material-rocket-launch: **[Usage](usage.md)** — SQL and PySpark quick starts.
- :material-source-branch: **[Architecture](architecture.md)** — how the platform stays
  outside Spark and pluggable across versions.
- :material-history: **[Titian](titian.md)** — record-level data provenance, the engine
  the other tools build on.
- :material-bug-check: **[BigSift](bigsift.md)** — the minimal fault-inducing input set.
- :material-cog: **[Getting started](install.md)** — build, test, deploy, notebooks.
- :material-book-open-variant: **[Command & flag reference](reference.md)** — every
  task, script, config flag and env var.
- :material-tune: **[Ablation & performance](ablation.md)** — tuning flags and the
  optimization study.
- :material-table-check: **[TPC-DS coverage](tpcds.md)** — the coverage harness and
  re-execution oracle.
- :material-hammer-wrench: **[Developer guide](developer-guide.md)** — how to extend
  capture, and how to add a Spark binding.
- :material-format-quote-close: **[Citations](citations.md)** — BibTeX for each tool.

</div>

## Design principles

**Stay outside Spark.** No forked Spark, no patched jars. Capture attaches through
sanctioned extension points — `spark.sql.extensions`, RDD subclassing, task-completion
listeners — plus a thin, documented internals layer.

**Stay pluggable across Spark versions.** Everything version-specific lives behind the
`SparkBinding` service interface in its own module, resolved at runtime by
`ServiceLoader`. Tools compile against `modules/api` alone, so supporting a future Spark
release means adding a binding module — not changing any tool. See
[Architecture](architecture.md).

**Fail loud, never silently wrong.** An operator outside the verified coverage set
aborts capture with a clear error rather than returning lineage that looks plausible and
is wrong.

**SQL and PySpark first.** These are the front ends people actually use.

## Tools

**integrated** means it builds, runs, and is covered by tests in this repository.

| Tool | What it does | Status |
|---|---|---|
| [Titian](titian.md) | Record-level provenance, backward and forward | **integrated** |
| [BigSift](bigsift.md) | Minimum failure-inducing input set | **integrated** |
| BigDebug | Simulated breakpoints and on-demand watchpoints | planned |
| FlowDebug | Influence-based provenance, taint inside UDFs | planned |
| OptDebug | Ranks the operations behind a wrong result | planned |
| PerfDebug | Attributes computation skew to the records causing it | planned |
| DeSQL | Step-through SQL debugging via query decomposition | planned |
| Vega | Incremental re-execution across query revisions | planned |
| BigTest | Symbolic execution over dataflow operators and UDFs | planned |
| BigFuzz | Fuzzing via framework abstraction | planned |
| DepFuzz | Co-dependence-aware mutation across joined tables | planned |
| NaturalFuzz | Splices existing rows and columns into realistic inputs | planned |
| NaturalSym | Distribution-aware symbolic test generation | planned |

Debugging tools are being migrated first, then the testing tools. The upstream
repository and commit each migrated tool derives from is recorded in
[PROVENANCE.md](https://github.com/BigAsterisk/BigAsterisk/blob/main/PROVENANCE.md).
