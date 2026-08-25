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

# an edit written inside a derived table sits below the join; Vega moves it above so
# the join itself stays reusable across the revision
v.clear()
spark.read.schema("cid STRING, name STRING") \
     .csv(os.path.join(DATA, "customers_csv")).createOrReplaceTempView("customers")


def joined(threshold):
    return ("SELECT o.oid, c.name FROM (SELECT * FROM orders WHERE amount > %d) o "
            "JOIN customers c ON o.cid = c.cid" % threshold)


expected_join = {(r["oid"], r["name"]) for r in spark.sql(joined(200)).collect()}
v.run(joined(100)).df.collect()
moved = v.run(joined(200))
check("the edit is moved later", moved.rewritten is True, repr(moved))
check("the join survives the edit", any("Join" in r for r in moved.reused), str(moved.reused))
check("moving the edit does not change the answer",
      {(r["oid"], r["name"]) for r in moved.df.collect()} == expected_join)
v.clear()
v.run(BASE).df.collect()

unrelated = v.run("SELECT oid FROM orders WHERE amount < 0")
check("unrelated query reuses nothing", unrelated.reused == [], str(unrelated.reused))

check("materialized is listed", len(v.materialized) > 0)
check("max_materialized is exposed", v.max_materialized > 0)
check("repr is readable", "VegaRun" in repr(second), repr(second))

v.clear()
check("clear releases everything", v.materialized == [])

print("\n%d checks passed" % len(passed))
spark.stop()
