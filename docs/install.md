# Getting started

## Requirements

- **JDK 17+** — a Spark 4 requirement
- **sbt 1.10+**
- **Apache Spark 4.1.x** — `spark-core` and `spark-sql` are `Provided`; BigAsterisk
  attaches to the Spark you already run
- **Python 3.10+** for the PySpark front end

`bin/bootstrap` fetches a project-local JDK, sbt and Spark distribution into `tools/`,
so the build never depends on what happens to be on your `PATH`. Nothing is installed
outside the repository.

```bash
git clone https://github.com/BigAsterisk/BigAsterisk.git
cd BigAsterisk
bin/bootstrap            # JDK 17 + sbt + Python 3.11 + Spark 4.1.2 (--no-spark skips ~400 MB)
```

## See it work

```bash
bin/bigasterisk tour
```

Runs every tool against one small dataset in a single process — the quickest check that
the build is sound, and the quickest way to see what each tool is for. It exits non-zero
if any tool fails, and CI runs it on every push.

## Build

```bash
bin/sbt package          # -> modules/*/target/scala-2.13/bigasterisk-*.jar
```

Three jars are produced — `bigasterisk-api`, `bigasterisk-spark4`, `bigasterisk-bigsift`
— plus one bundled runtime dependency, **fastutil**, which `sbt` resolves into the
coursier cache (`~/.cache/coursier` on Linux, `~/Library/Caches/Coursier` on macOS).

## Run the tests

```bash
bin/sbt test             # Scala suites, including local-cluster mode
python/tests/run.sh      # PySpark end-to-end
```

The `local-cluster` suites launch real executor JVMs through Spark's launcher — real
serialization, classpath distribution, remote block fetches — so they need the Spark
distribution that `bin/bootstrap` unpacks at `tools/spark-4.1.2-bin-hadoop3`. Set
`SPARK_HOME` to use a different one.

## Use it in an application

```bash
spark-submit \
  --jars bigasterisk-api.jar,bigasterisk-spark4.jar,bigasterisk-bigsift.jar,fastutil.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension,org.apache.spark.sql.bigdebug.BigDebugExtension \
  your-app.jar
```

Or let the platform configure the session for you, which is the same thing without the
class name in your build scripts:

```scala
val spark = BigAsterisk.configure(SparkSession.builder()).getOrCreate()
```

PySpark users ship the package with `--py-files`:

```bash
( cd python && zip -qr /tmp/bigasterisk.zip bigasterisk )
spark-submit --jars ... --py-files /tmp/bigasterisk.zip your_app.py
```

See [Usage](usage.md) for the API, and [Architecture](architecture.md) for how the
binding is selected.

## Notebooks

There is a notebook for each tool in `notebooks/`, plus deeper dives into the SQL and RDD
surfaces and one (`python_udfs.ipynb`) on reading inside a Python UDF.

**Start with `airline_analysis.ipynb`** — a quarter of a million flights, three joins, two
Python UDFs and one planted fault, with all thirteen tools taking turns on it. Each states
its question, its method and what it found, so it reads as an investigation rather than a
feature list. Each one runs on the committed fixtures and ends in assertions,
so executing them is a real check rather than a demo:

```bash
scripts/validate-notebooks.sh              # execute all of them
scripts/validate-notebooks.sh optdebug     # just one
```

CI runs this on every push.

**Docker** — self-contained, builds the jars from source:

```bash
docker build -t bigasterisk -f docker/Dockerfile .
docker run --rm -p 8888:8888 bigasterisk        # JupyterLab at http://localhost:8888
docker run --rm bigasterisk validate            # execute all notebooks headlessly
```

**Local**:

```bash
bin/sbt package
pip install pyspark==4.1.2 jupyterlab
cd notebooks && jupyter lab
```

The notebooks' bootstrap cell self-locates the jars, fastutil, data and `spark-shell`;
override with `BIGASTERISK_HOME` / `SPARK_HOME` if needed. Make sure `JAVA_HOME` points
at JDK 17+.

## API documentation (Scaladoc)

```bash
bin/sbt unidoc   # -> target/scala-2.13/unidoc/index.html
```

One Scaladoc site across every module: the public API grouped by task, plus the
capture-engine internals (tap operators, runtimes, block formats).

## Documentation site

```bash
pip install mkdocs-material

# The manual links to api/index.html, so generate the Scaladoc into docs/api first.
bin/sbt unidoc && rm -rf docs/api && cp -r target/scala-2.13/unidoc docs/api

mkdocs serve                 # http://127.0.0.1:8000
mkdocs build --strict        # what CI runs: broken links fail the build
```

## TPC-DS coverage harness

```bash
pip install duckdb
python3 tpcds/gen_data.py 0.2          # ~200 MB of Parquet under tpcds/data/sf0.2
bin/sbt 'examples/runMain org.bigasterisk.examples.TPCDSCoverage'
# or a subset / another data directory:
bin/sbt 'examples/runMain org.bigasterisk.examples.TPCDSCoverage <dataDir> <queryDir> q3,q7'
```

Per query it checks capture-on answers against capture-off, traces one result row back
to a scan, and prints a PASS / UNSUPPORTED / ERROR scoreboard. See
[TPC-DS coverage](tpcds.md).

## Cluster deployment notes

- Ship the BigAsterisk jars and `fastutil`; everything else comes from Spark.
- Lineage is materialized into executor BlockManagers. **Avoid dynamic allocation or
  executor preemption during a capture-and-trace session** — losing an executor loses
  its lineage partitions.
- `show()` resolves traces by deterministically re-scanning the source files, so results
  are valid while the input files are unchanged.
- Capture supports a defined operator set and **fails loudly** on anything outside it,
  never silent wrong lineage. Display queries (`df.show()`, `LIMIT`) skip capture.
- Release a query's lineage blocks when you are done: `releaseLineage(df)` in Scala,
  `release_lineage(df)` in Python.
