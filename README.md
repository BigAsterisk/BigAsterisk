<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)"
            srcset="docs/assets/bigasterisk-horizontal-dark.svg">
    <img src="docs/assets/bigasterisk-horizontal.svg" alt="BigAsterisk" width="420">
  </picture>
</p>

[![CI](https://github.com/BigAsterisk/BigAsterisk/actions/workflows/ci.yml/badge.svg)](https://github.com/BigAsterisk/BigAsterisk/actions/workflows/ci.yml)
[![Documentation](https://github.com/BigAsterisk/BigAsterisk/actions/workflows/docs.yml/badge.svg)](https://bigasterisk.github.io/BigAsterisk/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Spark](https://img.shields.io/badge/Apache%20Spark-4.1.x-e25a1c.svg)](https://spark.apache.org/)
[![Scala](https://img.shields.io/badge/Scala-2.13-red.svg)](https://www.scala-lang.org/)

**A unified debugging and testing platform for Apache Spark.**

BigAsterisk brings thirteen research systems for debugging and testing data-intensive
scalable computing — built between 2016 and 2024 at UCLA and Virginia Tech — into a
single, maintained codebase that runs on **stock Apache Spark**.

Each of these tools originally shipped as its own fork of Spark, pinned to the release
it was written against (Spark 1.2, 2.1, 2.4, 3.0). That is why almost none of them can
be run today. BigAsterisk replaces the forks with one library that attaches to an
unmodified Spark distribution through a versioned binding layer, so the techniques
outlive the release they were written for.

```scala
val spark   = BigAsterisk.configure(SparkSession.builder().master("local[*]")).getOrCreate()
val lineage = BigAsterisk.lineage(spark)
lineage.enableCapture(spark)

val df = spark.sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category")
val rows = lineage.collectWithLineage(df)             // (Row, lineage id)

lineage.trace(df, Seq(rows.head._2))                  // trace one output row ...
       .goBack().show(full = true).foreach(println)   // ... back to its source records
lineage.releaseLineage(df)
```

## Design principles

**Stay outside Spark.** No forked Spark, no patched jars. Capture attaches through
sanctioned extension points — `spark.sql.extensions`, RDD subclassing, task-completion
listeners — plus a thin, documented internals layer. Your jobs run on the Spark you
already have.

**Stay pluggable across Spark versions.** Everything version-specific lives behind the
[`SparkBinding`](modules/api/src/main/scala/org/bigasterisk/api/SparkBinding.scala)
service interface in its own module, resolved at runtime by `ServiceLoader`. Tools
compile against `modules/api` alone. Supporting a future Spark release means adding a
`modules/sparkN` binding — no tool changes:

```
modules/api  ──►  SparkBinding (SPI)  ──►  modules/spark4   Spark 4.x
      ▲                                    modules/spark5   (future)
      │
  every tool compiles against api only
```

**Fail loud, never silently wrong.** An operator outside the verified coverage set
aborts capture with a clear error rather than returning lineage that looks plausible
and is wrong.

**SQL and PySpark first.** These are the front ends people actually use; the RDD API
is supported where the original technique required it.

## Tools

Status is reported honestly: **integrated** means it builds, runs, and is covered by
tests in this repository. Where a paper's mechanism could not be carried over intact,
the tool's page says so and says what replaced it.

| Tool | What it does | Input | Status |
|---|---|---|---|
| [Titian](docs/titian.md) | Record-level provenance, backward and forward | application | **integrated** |
| [BigSift](docs/bigsift.md) | Minimum failure-inducing input set (provenance + delta debugging) | output, oracle | **integrated** |
| [BigDebug](docs/bigdebug.md) | Breakpoints, watchpoints, crash-culprit determination, and a debugging tab in the Spark UI | application | **integrated** |
| [FlowDebug](docs/flowdebug.md) | Influence-based provenance, taint propagated inside UDFs | application | **integrated** — Python UDFs read from source, Scala/Java from bytecode |
| [OptDebug](docs/optdebug.md) | Code-space fault isolation: ranks the operations behind a wrong result | output, oracle | **integrated** — branches inside UDFs are scored as operations |
| [PerfDebug](docs/perfdebug.md) | Attributes computation skew to the records causing it | execution metrics | **integrated** |
| [DeSQL](docs/desql.md) | Step-through SQL debugging via automated query decomposition | SQL query | **integrated** |
| [Vega](docs/vega.md) | Incremental re-execution across successive query revisions | query history | **integrated** — reimplemented from the paper |
| [BigTest](docs/testgen.md) | Symbolic execution over dataflow operators and UDFs | application | **integrated** — a UDF's own paths are among the solved conditions |
| [BigFuzz](docs/fuzzing.md) | Fuzzing via framework abstraction | application | **integrated** |
| [DepFuzz](docs/fuzzing.md) | Co-dependence-aware mutation, so inputs survive joins across tables | input data | **integrated** |
| [NaturalFuzz](docs/fuzzing.md) | Splices existing rows and columns into realistic inputs | input data | **integrated** |
| [NaturalSym](docs/testgen.md) | Symbolic execution that generates natural, distribution-aware inputs | application | **integrated** |

All thirteen now run, and each tool's page states plainly where the implementation
departs from its paper and why — in several cases the original mechanism required a
forked Spark, a JDK 8 symbolic execution engine, or a benchmark suite that no longer
exists, and was rebuilt on a supported extension point instead. Every tool records the
upstream repository and commit it derives from in
[PROVENANCE.md](PROVENANCE.md), and documents any deviation from the published
technique.

## Getting started

**[docs/setup.md](docs/setup.md)** covers the three ways in — a cluster with notebooks on
top of it, notebooks alone, or a checkout for building and changing the code. None of
them asks you to install Spark, a JDK or Python by hand.

### Notebooks on a real cluster

```bash
scripts/cluster.sh up 3     # master + 3 workers + history + JupyterLab on :8888
```

JupyterLab at <http://localhost:8888> drives that cluster: the notebooks run in the
driver, the work lands on the workers, and <http://localhost:8080> shows it happening.

### Your own job, from the command line

```bash
bin/bigasterisk analyze \
  --table orders=examples/data/orders.txt \
  --schema orders="oid STRING, cid STRING, amount INT" \
  --query "SELECT cid, SUM(amount) AS total FROM orders GROUP BY cid" \
  --tool desql          # or titian, bigsift, optdebug, bigtest, fuzz, ... or all
```

Point it at any tables and any SQL; `scripts/cluster.sh analyze ...` runs the same thing
against the cluster. The bundled tour and benchmarks exist to check the platform still
works, not to define what it works on.

```bash
scripts/cluster.sh tour     # smoke test: all 13 tools on the bundled example
scripts/cluster.sh down
```

**[The worked example](https://bigasterisk.github.io/BigAsterisk/notebook/)** —
readable in a browser with all its output, no install — is the fullest demonstration: a quarter of a million flights, two joins, two Python UDFs, and one
fault planted where it makes the answer look *almost* right. Thirteen tools take turns on
it, each narrating what it is doing — the question narrows from "something is wrong" to
"this record, and this branch of this function".

**[docs/demo.md](docs/demo.md)** walks through every tool one at a time with the output
each produces — including the fuzzers side by side (19 of 20 iterations wasted against 0
of 20), reading inside a Python UDF, and reproducing Titian's under-30% capture overhead
on two million rows. Every command there was run to produce the output shown.

### From a checkout

Requirements: JDK 17+, and a Spark 4.1.x installation if you want to run on a cluster.
`bin/bootstrap` fetches a project-local JDK, sbt and Spark distribution into `tools/`
so the build never depends on what happens to be on your `PATH`:

```bash
bin/bootstrap          # one-time: JDK 17 + sbt + Python + Spark 4.1.2 into tools/
bin/bigasterisk tour   # every tool, on one small dataset, in one run
bin/sbt test           # build and run the test suites
```

The tour is the fastest way to see what the platform does. It runs a query with a
planted fault and lets each tool answer a different question about it:

```
── FlowDebug — which of them actually mattered
  0.9936  [o8,c2,99999]  (contribution 99.4% of the total magnitude)
── BigSift — which input records are to blame (data-space)
  provenance left 4 candidate records; delta debugging narrowed them to 1
    [o8,c2,99999]
── OptDebug — which operation is to blame (code-space)
  1.000  [1] Aggregate branch — (amount > 1000)  (failing=1, passing=0)
── BigDebug — which record killed the query
  partition 0, record 7: [o8,c2,99999]
```

Each tool answers one question about the failure, in its own terms.

### Two lines in your application, the rest in the browser

Attaching the platform is one call. It also installs a **BigAsterisk tab in the Spark
UI**, next to Jobs and Stages, so watchpoint hits, breakpoints, the record that killed a
task, per-record latency and what was read inside your UDFs are all visible while the
job runs — no print statements, no second submission:

```python
import bigasterisk
spark = bigasterisk.configure(SparkSession.builder).getOrCreate()
```

The tools take your pipeline as it is. A DataFrame is a query:

```python
result = flights.filter(col("cancelled") == 0).groupBy("carrier").agg(avg("delay"))

bigasterisk.desql(spark).decompose(result)            # its steps, each runnable
bigasterisk.influence(spark).influencers(result, "avg_delay < -20")
bigasterisk.fuzz(spark).fuzz(result, {"flights": flights})
```

Nothing has to be rewritten as a SQL string first — including for the tools that re-run
your query hundreds of times with data of their own. See
[docs/usage.md](docs/usage.md).

To use BigAsterisk from your own application:

```bash
spark-submit \
  --jars bigasterisk-api.jar,bigasterisk-spark4.jar,bigasterisk-bigsift.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension,org.apache.spark.sql.bigdebug.BigDebugExtension \
  --conf spark.plugins=org.bigasterisk.spark4.BigAsteriskPlugin \
  your-app.jar
```

PySpark users ship `python/bigasterisk` with `--py-files` and use
`bigasterisk.lineage(spark)`. See [docs/usage.md](docs/usage.md).

## Repository layout

| Path | Contents |
|---|---|
| `modules/api` | Version-independent tool API and the `SparkBinding` SPI |
| `modules/spark4` | Spark 4.x binding: the lineage capture engine (RDD taps and SQL codegen taps) |
| `modules/bigsift` | Fault-inducing input isolation |
| `modules/optdebug` | Fault-inducing *operation* isolation (depends only on `api`) |
| `python/bigasterisk` | PySpark front end |
| `examples` | The platform tour, plus per-tool demos and benchmarks |
| `tpcds` | TPC-DS coverage harness and re-execution oracle |
| `notebooks` | A runnable notebook for each tool, executed in CI |
| `docs` | Documentation site (`mkdocs serve`) |

## Publications

<details>
<summary>The thirteen systems and their papers</summary>

- **Titian: Data Provenance Support in Spark.** Matteo Interlandi, Kshitij Shah, Sai
  Deep Tetali, Muhammad Ali Gulzar, Seunghyun Yoo, Miryung Kim, Todd Millstein, Tyson
  Condie. *PVLDB* 9(3), 2016.
- **BigDebug: Debugging Primitives for Interactive Big Data Processing in Spark.**
  Muhammad Ali Gulzar, Matteo Interlandi, Seunghyun Yoo, Sai Deep Tetali, Tyson Condie,
  Todd Millstein, Miryung Kim. *ICSE* 2016.
- **Optimizing Interactive Development of Data-Intensive Applications** (Vega). Matteo
  Interlandi, Sai Deep Tetali, Muhammad Ali Gulzar, Joseph Noor, Tyson Condie, Miryung
  Kim, Todd Millstein. *SoCC* 2016.
- **Automated Debugging in Data-Intensive Scalable Computing** (BigSift). Muhammad Ali
  Gulzar, Matteo Interlandi, Xueyuan Han, Mingda Li, Tyson Condie, Miryung Kim.
  *SoCC* 2017.
- **White-Box Testing of Big Data Analytics with Complex User-Defined Functions**
  (BigTest). Muhammad Ali Gulzar, Shaghayegh Mardani, Madanlal Musuvathi, Miryung Kim.
  *ESEC/FSE* 2019.
- **PerfDebug: Performance Debugging of Computation Skew in Dataflow Systems.** Jason
  Teoh, Muhammad Ali Gulzar, Guoqing Harry Xu, Miryung Kim. *SoCC* 2019.
- **Influence-Based Provenance for Dataflow Applications with Taint Propagation**
  (FlowDebug). Jason Teoh, Muhammad Ali Gulzar, Miryung Kim. *SoCC* 2020.
- **BigFuzz: Efficient Fuzz Testing for Data Analytics Using Framework Abstraction.**
  Qian Zhang, Jiyuan Wang, Muhammad Ali Gulzar, Rohan Padhye, Miryung Kim. *ASE* 2020.
- **OptDebug: Fault-Inducing Operation Isolation for Dataflow Applications.** Muhammad
  Ali Gulzar, Miryung Kim. *SoCC* 2021.
- **Co-dependence Aware Fuzzing for Dataflow-Based Big Data Analytics** (DepFuzz).
  Ahmad Humayun, Miryung Kim, Muhammad Ali Gulzar. *ESEC/FSE* 2023.
- **NaturalFuzz: Natural Input Generation for Big Data Analytics.** Ahmad Humayun,
  Yaoxuan Wu, Miryung Kim, Muhammad Ali Gulzar. *ASE* 2023.
- **DeSQL: Interactive Debugging of SQL in Data-Intensive Scalable Computing.** Sabaat
  Haroon, Chris Brown, Muhammad Ali Gulzar. *FSE* 2024.
- **Natural Symbolic Execution-based Testing for Big Data Analytics** (NaturalSym).
  Yaoxuan Wu, Ahmad Humayun, Muhammad Ali Gulzar, Miryung Kim. *FSE* 2024.

</details>

If you use BigAsterisk in academic work, please cite the paper for the specific
technique you used. Per-tool BibTeX entries are in [docs/citations.md](docs/citations.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Two rules shape most of what a good change looks
like here: nothing that knows about a specific Spark version may live in a tool, and a
tool that cannot answer must say so rather than approximate.

Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md). To report a
vulnerability, see [SECURITY.md](SECURITY.md).

## License

Apache License 2.0. Portions derive from the original Titian and BigDebug forks of
Apache Spark and retain that project's license and headers.
