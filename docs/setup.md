# Setup

Three ways in, depending on what you already have. The first needs nothing but Docker;
the last is for working on the platform itself.

| | You want to | You need | Time |
|---|---|---|---|
| **A** | notebooks **and** a real cluster behind them | Docker | ~10 min, mostly the build |
| **B** | notebooks only, no cluster | Docker | ~10 min |
| **C** | build, test, or change the code | a checkout | ~15 min |

None of them asks you to install Spark, a JDK or Python by hand.

---

## A. Notebooks on a real cluster

One command brings up a standalone master, workers in their own containers, a history
server, and JupyterLab whose Spark driver is a **client of that cluster** — so the
notebooks run where the tools are meant to run.

```bash
git clone https://github.com/BigAsterisk/BigAsterisk.git
cd BigAsterisk

scripts/cluster.sh up 3
```

The first run builds the image: it compiles the platform from source and downloads Spark,
so allow several minutes. Afterwards it is cached and `up` takes seconds.

```
notebooks:  http://localhost:8888   <- JupyterLab, driving this cluster
master UI:  http://localhost:8080   (expect 3 workers registered)
history:    http://localhost:18080
```

Open <http://localhost:8888> — no token, no password — and start with
**`airline_analysis.ipynb`**: a quarter of a million flights, three joins, two Python UDFs
and one planted fault, with all thirteen tools taking turns on it. Watch
<http://localhost:8080> while it runs and you will see the application appear and the
work land on the workers.

Also available while the cluster is up:

```bash
scripts/cluster.sh analyze --help   # any tool, against YOUR query and data
scripts/cluster.sh tour             # smoke test: 13 tools on the bundled example
scripts/cluster.sh down
```

`local[*]` runs every executor inside the driver JVM, which hides the failures that only
appear when executors are genuinely elsewhere — which is why this is the path the docs
lead with. [Running on a cluster](cluster.md) explains the parts.

!!! tip "Give Docker enough memory and disk"

    The airline notebook generates 250,000 flights across three workers. 6 GB of memory
    is comfortable, and the image plus Spark needs a few GB of disk. Docker Desktop →
    Settings → Resources. To make the run smaller, set `FLIGHTS` in the notebook's first
    code cell.

---

## B. Notebooks without a cluster

A single container running JupyterLab with Spark in local mode. Smaller and simpler; it
just cannot show you anything that only happens when executors are elsewhere.

```bash
docker build -t bigasterisk -f docker/Dockerfile .
docker run --rm -p 8888:8888 bigasterisk
```

To check every notebook still passes without opening a browser:

```bash
docker run --rm bigasterisk validate
```

Each notebook ends in assertions, so a clean run is a real check rather than a demo.

---

## C. From a checkout

For building, testing, or changing the platform.

```bash
git clone https://github.com/BigAsterisk/BigAsterisk.git
cd BigAsterisk

bin/bootstrap        # JDK 17, sbt, Python and Spark into tools/
bin/sbt package      # build the jars the notebooks and scripts load
```

`bin/bootstrap` fetches everything into `tools/`, which is gitignored, so the build never
depends on what happens to be on your `PATH` and nothing is installed system-wide. Pass
`--no-spark` to skip the ~400 MB Spark download if you already have one; then set
`SPARK_HOME` yourself.

**Requirements:** a JDK is *not* required beforehand — bootstrap fetches one. If you would
rather use your own, it must be **JDK 17 or newer**, and Python must be **3.10+** (PySpark
4.1 uses PEP 604 unions at import time).

### Running things

```bash
bin/bigasterisk analyze --help          # any tool, against your own query and data
bin/bigasterisk tour                    # smoke test: every tool, one bundled dataset
scripts/standalone-tour.sh              # the same, on a real standalone cluster
python/demos/run-airline.sh             # the airline analysis as a script
scripts/validate-notebooks.sh           # execute every notebook headlessly
scripts/validate-notebooks.sh airline_analysis    # just one

bin/sbt test                            # the Scala suites
python/tests/run.sh                     # the PySpark end-to-end tests
bin/sbt 'benchmarks/runMain org.bigasterisk.benchmarks.BenchmarkRunner'
```

### Notebooks from a checkout

Notebooks find the jars themselves — they glob `modules/*/target` — so `bin/sbt package`
is the only build prerequisite.

To **execute** them headlessly, nothing more is needed: `bin/bootstrap` installs
`nbconvert` and the runner sets `SPARK_HOME` and `PYTHONPATH` itself.

```bash
bin/sbt package
scripts/validate-notebooks.sh airline_analysis     # or omit the name for all of them
```

To **open them in a browser**, install JupyterLab into the project's interpreter —
bootstrap deliberately installs only what the headless runner needs:

```bash
tools/python/bin/python3 -m pip install jupyterlab
tools/python/bin/python3 -m jupyterlab --notebook-dir notebooks
```

Or use your own Python, which then needs `jupyterlab` *and* `pyspark==4.1.2` (or
`SPARK_HOME` pointing at a Spark 4.1.x distribution), plus `BIGASTERISK_HOME` set to the
repository root if you start Jupyter from elsewhere.

If you only want to read a notebook in a browser without any of this, **option A is the
shorter road** — the image already has JupyterLab in it.

---

## Attaching the platform to your own Spark

Nothing above is required to *use* BigAsterisk. It attaches to a Spark you already run:

```bash
spark-submit \
  --jars bigasterisk-api.jar,bigasterisk-spark4.jar,bigasterisk-bigsift.jar,bigasterisk-optdebug.jar,fastutil.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension \
  your-application.jar
```

Add the extension for the tool you want — each tool's page lists its own. Spark itself is
never patched and never restarted with a modified build.

From Python:

```python
import bigasterisk
spark = bigasterisk.configure(SparkSession.builder).getOrCreate()
```

---

## When something does not work

**`No BigAsterisk jars found. Run: bin/sbt package`** — a notebook could not find the
jars. Run `bin/sbt package` from the repository root, or set `BIGASTERISK_HOME` if you
started Jupyter from somewhere else.

**`fastutil jar not found`** — the same fix. The jar comes from the coursier cache that
sbt populates (`~/.cache/coursier` on Linux, `~/Library/Caches/Coursier` on macOS), or
from `jars/` inside the Docker image. `FASTUTIL_JAR` overrides the search.

**`PySpark 4.1 needs Python 3.10+`** — set `PYSPARK_PYTHON` to a newer interpreter, or use
the one in `tools/python/bin/python3`.

**A notebook cell fails with `PATH_NOT_FOUND`** — Jupyter was started somewhere other than
`notebooks/`. Set `BIGASTERISK_HOME` to the repository root.

**The cluster starts but no workers register** — `scripts/cluster.sh status` reports what
the master actually sees. A container can be up while its worker failed to register.

**Something else** — [open an issue](https://github.com/BigAsterisk/BigAsterisk/issues)
with the command you ran and what it printed.
