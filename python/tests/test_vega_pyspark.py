# End-to-end PySpark test for incremental re-execution across query revisions.
#
# Run through python/tests/run.sh, which builds the jars, zips the package and
# submits this file.

import os
import sys

from pyspark.sql import SparkSession

import bigasterisk

passed = []


def check(name, cond, detail=""):
    if cond:
        passed.append(name)
        print("PASS  %s" % name)
    else:
        print("FAIL  %s  %s" % (name, detail))
        sys.exit(1)


spark = bigasterisk.configure(SparkSession.builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

ROOT = os.environ.get("BIGASTERISK_HOME", ".")
DATA = os.path.join(ROOT, "modules/spark4/src/test/resources")
spark.read.schema("oid STRING, cid STRING, amount INT") \
     .csv(os.path.join(DATA, "orders_csv")).createOrReplaceTempView("orders")

v = bigasterisk.vega(spark)
v.clear()

BASE = "SELECT cid, amount FROM orders WHERE amount > 100"
REVISED = "SELECT cid, SUM(amount) AS total FROM orders WHERE amount > 100 GROUP BY cid"

# the truth, with no materialization involved
expected = {(r["cid"], r["total"]) for r in spark.sql(REVISED).collect()}

first = v.run(BASE)
first.df.collect()
check("first run reuses nothing", first.reused == [], str(first.reused))
check("first run materializes something", len(first.materialized) > 0)

second = v.run(REVISED)
check("revision reuses the shared part", len(second.reused) > 0, str(second.reused))
check("reused part is the filter",
      any("Filter" in r for r in second.reused), str(second.reused))
check("reuse_ratio is reported", 0.0 < second.reuse_ratio <= 1.0, str(second.reuse_ratio))

actual = {(r["cid"], r["total"]) for r in second.df.collect()}
check("reuse does not change the answer", actual == expected,
      "%r vs %r" % (actual, expected))

unrelated = v.run("SELECT oid FROM orders WHERE amount < 0")
check("unrelated query reuses nothing", unrelated.reused == [], str(unrelated.reused))

check("materialized is listed", len(v.materialized) > 0)
check("max_materialized is exposed", v.max_materialized > 0)
check("repr is readable", "VegaRun" in repr(second), repr(second))

v.clear()
check("clear releases everything", v.materialized == [])

print("\n%d checks passed" % len(passed))
spark.stop()
