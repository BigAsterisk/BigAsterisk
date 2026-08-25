# BigAsterisk

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
tests in this repository.

| Tool | What it does | Input | Status |
|---|---|---|---|
| [Titian](docs/titian.md) | Record-level provenance, backward and forward | application | **integrated** |
| [BigSift](docs/bigsift.md) | Minimum failure-inducing input set (provenance + delta debugging) | output, oracle | **integrated** |
| BigDebug | Simulated breakpoints and on-demand watchpoints over distributed intermediate data | application | planned |
| FlowDebug | Influence-based provenance, taint propagated inside UDFs | application | planned |
| OptDebug | Code-space fault isolation: ranks the operations behind a wrong result | output, oracle | planned |
| PerfDebug | Attributes computation skew to the records causing it | execution metrics | planned |
| [DeSQL](docs/desql.md) | Step-through SQL debugging via automated query decomposition | SQL query | **integrated** |
| Vega | Incremental re-execution across successive query revisions | query history | planned |
| BigTest | Symbolic execution over dataflow operators and UDFs | application | planned |
| BigFuzz | Fuzzing via framework abstraction | application | planned |
| DepFuzz | Co-dependence-aware mutation, so inputs survive joins across tables | input data | planned |
| NaturalFuzz | Splices existing rows and columns into realistic inputs | input data | planned |
| NaturalSym | Symbolic execution that generates natural, distribution-aware inputs | application | planned |

Debugging tools are being migrated first, then the testing tools. Every migrated tool
records the upstream repository and commit it derives from in
[PROVENANCE.md](PROVENANCE.md), and documents any deviation from the published
technique.

## Getting started

Requirements: JDK 17+, and a Spark 4.1.x installation if you want to run on a cluster.
`bin/bootstrap` fetches a project-local JDK, sbt and Spark distribution into `tools/`
so the build never depends on what happens to be on your `PATH`:

```bash
bin/bootstrap          # one-time: JDK 17 + sbt + Spark 4.1.2 into tools/
bin/sbt test           # build and run the test suites
```

To use BigAsterisk from your own application:

```bash
spark-submit \
  --jars bigasterisk-api.jar,bigasterisk-spark4.jar,bigasterisk-bigsift.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension \
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
| `python/bigasterisk` | PySpark front end |
| `examples` | Runnable demos and benchmarks |
| `tpcds` | TPC-DS coverage harness and re-execution oracle |
| `notebooks` | End-to-end notebooks (SQL, PySpark, RDD) |
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

## License

Apache License 2.0. Portions derive from the original Titian and BigDebug forks of
Apache Spark and retain that project's license and headers.
