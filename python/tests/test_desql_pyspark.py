# End-to-end PySpark test for step-through SQL debugging.
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
spark.read.schema("cid STRING, name STRING") \
     .csv(os.path.join(DATA, "customers_csv")).createOrReplaceTempView("customers")

d = bigasterisk.desql(spark)

QUERY = ("SELECT c.name, SUM(o.amount) AS total "
         "FROM orders o JOIN customers c ON o.cid = c.cid "
         "WHERE o.amount > 100 GROUP BY c.name")

steps = d.decompose(QUERY)
check("decompose returns steps", len(steps) > 0, "got %d" % len(steps))

ops = [s.operator for s in steps]
check("plan operators are exposed",
      "Filter" in ops and "Join" in ops and "Aggregate" in ops, str(ops))

check("children precede their consumer",
      all(c < s.id for s in steps for c in s.child_ids), str(ops))

check("aliases are folded away", "SubqueryAlias" not in ops, str(ops))

flt = next(s for s in steps if s.operator == "Filter")
check("filter step drops the rows it should",
      flt.data.count() == 8, "got %d" % flt.data.count())

last = steps[-1]
expected = {(r["name"], r["total"]) for r in spark.sql(QUERY).collect()}
actual = {(r["name"], r["total"]) for r in last.data.collect()}
check("last step reproduces the query answer", actual == expected,
      "%r vs %r" % (actual, expected))

check("repr is readable", "Filter" in repr(flt), repr(flt))

print("\n%d checks passed" % len(passed))
spark.stop()
