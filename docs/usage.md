# Usage

Build the session through `BigAsterisk.configure` so the binding for your Spark version
installs itself, then reach the tools through `BigAsterisk`. SQL and PySpark are the
primary front ends; the RDD API is available where the original technique needed it.

## The command line

Every tool is reachable from one command, against any tables and any SQL:

```bash
bin/bigasterisk analyze \
  --table orders=examples/data/orders.txt \
  --schema orders="oid STRING, cid STRING, amount INT" \
  --query "SELECT cid, SUM(amount) AS total FROM orders GROUP BY cid" \
  --tool desql
```

`--tool all` runs every one. `flowdebug`, `bigsift` and `optdebug` also need `--oracle`,
a SQL predicate over the query's output that is true for a *wrong* row — they have to
know which results are wrong, and only you can say. `bin/bigasterisk analyze --help`
lists the flags.

The rest of this page is the programmatic API, for using the tools from your own code.


## Spark SQL / DataFrames

```scala
import org.apache.spark.sql.SparkSession
import org.bigasterisk.api.BigAsterisk

val spark = BigAsterisk
  .configure(SparkSession.builder().master("local[*]"))   // registers the extension
  .getOrCreate()

val lineage = BigAsterisk.lineage(spark)
lineage.enableCapture(spark)

val df = spark.sql(
  """SELECT c.name, SUM(o.amount) AS total
    |FROM orders o JOIN customers c ON o.cid = c.cid
    |GROUP BY c.name""".stripMargin)

val output = lineage.collectWithLineage(df)          // Array[(Row, lineageId)]
val bobId  = output.find(_._1.getString(0) == "Bob").get._2

var cursor = lineage.trace(df, Seq(bobId))
cursor = cursor.goBack()                  // through the aggregation exchange
val orders    = cursor.goBack(0).show()   // culprit rows in orders
val customers = cursor.goBack(1).show()   // culprit rows in customers

lineage.releaseLineage(df)                // drop this query's lineage blocks
```

`goBack(branch)` selects which input to follow at a join; `0` is the build side.
`goNext()` retraces forward. `show(full = true)` returns complete source records rather
than the query's column-pruned view.

!!! note "Capture needs a real source"
    Lineage is captured at stage boundaries over a scan the engine can re-read. An
    in-memory `LocalTableScan` — what `Seq(...).toDF` produces — has no pre-exchange
    tap and is refused outright rather than traced incorrectly. Read from a file, a
    table, or a cached DataFrame.

## PySpark

```python
import bigasterisk
from pyspark.sql import SparkSession

spark = bigasterisk.configure(SparkSession.builder).getOrCreate()

lin = bigasterisk.lineage(spark)
lin.enable_capture()

df = spark.sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category")
rows_with_ids = lin.collect_with_lineage(df)

bad_id = next(i for r, i in rows_with_ids if r.total > 100000)
print(lin.trace(df, [bad_id]).to_scan().show(full=True))   # witnesses, as dicts
lin.release_lineage(df)
```

`to_scan()` walks back to the source in one call, following branch 0 at every join;
use `go_back(branch)` when you need to choose.

Python UDFs are traced exactly — the eval nodes are 1:1, and record ids are re-threaded
across their batching. Cached DataFrames (`df.cache()`) act as source boundaries: traces
stop at, and `show()` re-reads, the cached rows.

## A DataFrame is a query

Every tool takes your pipeline in whichever form you built it — a DataFrame or a SQL
string. That is worth stating explicitly because for several of them it is not free.

Fuzzing, test generation and delta debugging are search procedures: they propose an
input, run the query with it, look at the result, and repeat, hundreds of times. A SQL
string re-runs by being re-parsed, so whatever is registered under `flights` at the time
is what it reads. A DataFrame cannot: by the time you hold one its plan is analysed and
bound, and `flights` has stopped being a name.

So a DataFrame is re-run by substituting into its plan. The consequence for you is one
rule: **each seed must be the DataFrame the pipeline was built from**, or a table it
reads under that name, because that is what locates the leaf to replace.

```python
flights = spark.table("flights")
carriers = spark.table("carriers")

late = (flights.filter(col("arr_delay") > 60)
        .join(carriers, flights["carrier"] == carriers["code"])
        .groupBy(carriers["name"]).agg(count("*").alias("n")))

bigasterisk.fuzz(spark).fuzz(late, {"flights": flights, "carriers": carriers})
bigasterisk.testgen(spark).generate(late, {"flights": flights, "carriers": carriers})
bigasterisk.optdebug(spark).localize(late, "n > 1000", base_table="flights")
bigasterisk.BigSift(spark).debug("flights", late, lambda r: r["n"] > 1000)
```

Pass a seed the plan does not read and you get an error naming it, not a quiet result
about the original data:

```
Fuzzing cannot substitute data for shipments: the query's plan does not read them.
```

Substituting into a DataFrame's plan registers nothing and changes nothing in your
session. Substituting under a SQL string does register temporary views while it runs,
and puts the originals back afterwards.

## BigSift — automated fault isolation

```python
import bigasterisk

spark.read.csv(path, schema="category STRING, amount INT") \
     .createOrReplaceTempView("sales")

result = bigasterisk.BigSift(spark).debug(
    "sales",
    "SELECT category, SUM(amount) AS total FROM sales GROUP BY category",
    lambda r: r["total"] < 0)          # faulty = negative total

print(result.fault_inducing_rows)      # the minimal set of corrupt rows
```

See [BigSift](bigsift.md) for the Scala API and the algorithm.

## RDD API

```scala
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

val lc = new LineageContext(sparkContext)
lc.setCaptureLineage(true)

val totals = lc.textFile("sales.txt", 4)
  .map { line => val p = line.split(","); (p(0), p(1).toInt) }
  .reduceByKey(_ + _)
val output = totals.collectWithId()
lc.setCaptureLineage(false)

// trace the suspicious aggregate back to its exact input lines
var lin = totals.getLineage().filter(_ == suspiciousId)
lin = lin.goBack().goBack()
lin.show().collect().foreach(println)   // the culprit source lines
```

The RDD API reaches the engine directly rather than through the binding, because the
`LineageContext` wrapper is itself version-specific. Prefer SQL or PySpark for new work.

Runnable demos live in `examples/` (`SalesAnalysis`, `OrderCustomerJoin`).

## Deploying on a cluster

```bash
spark-submit \
  --jars bigasterisk-api.jar,bigasterisk-spark4.jar,bigasterisk-bigsift.jar,fastutil.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension,org.apache.spark.sql.bigdebug.BigDebugExtension \
  --conf spark.plugins=org.bigasterisk.spark4.BigAsteriskPlugin \
  your-app.jar
```

Everything else is `provided` by Spark. Lineage blocks live in executor BlockManagers,
so **avoid dynamic allocation or executor preemption during a capture-and-trace
session** — losing an executor loses its lineage partitions. Release per-query lineage
with `releaseLineage` when you are done with it. See
[Getting started](install.md#cluster-deployment-notes) for the full notes.
