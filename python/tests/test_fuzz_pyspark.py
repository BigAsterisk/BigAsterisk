# End-to-end PySpark test for fuzz testing.
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

orders = spark.read.schema("oid STRING, cid STRING, amount INT") \
              .csv(os.path.join(DATA, "orders_csv"))
customers = spark.read.schema("cid STRING, name STRING") \
                 .csv(os.path.join(DATA, "customers_csv"))
orders.createOrReplaceTempView("orders")
customers.createOrReplaceTempView("customers")

QUERY = ("SELECT c.name, SUM(o.amount) AS total "
         "FROM orders o JOIN customers c ON o.cid = c.cid "
         "WHERE o.amount > 100 GROUP BY c.name")

f = bigasterisk.fuzz(spark)
seeds = {"orders": orders, "customers": customers}

result = f.fuzz(QUERY, seeds, iterations=10, seed=1)
check("the campaign runs", result.iterations == 10, str(result.iterations))
check("branches are found", result.total_branches > 0, str(result.total_branches))
check("branches are reached", len(result.covered) > 0, str(result.covered))
check("coverage is a fraction", 0.0 < result.coverage <= 1.0, str(result.coverage))
check("repr is readable", "FuzzResult" in repr(result), repr(result))

# DepFuzz's actual contribution: random join keys never match, shared pools do
rnd = f.fuzz(QUERY, seeds, iterations=15, strategy="random", seed=7)
dep = f.fuzz(QUERY, seeds, iterations=15, strategy="co-dependent", seed=7)
check("co-dependent mutation survives the join",
      rnd.empty_results > dep.empty_results,
      "random=%d co-dependent=%d" % (rnd.empty_results, dep.empty_results))

nat = f.fuzz("SELECT cid, amount FROM orders", {"orders": orders},
             iterations=5, strategy="natural", seed=3)
check("natural strategy runs", nat.iterations == 5)

a = f.fuzz(QUERY, seeds, iterations=8, seed=42)
b = f.fuzz(QUERY, seeds, iterations=8, seed=42)
check("campaigns are reproducible", a.covered == b.covered, str((a.covered, b.covered)))

check("the caller's views are restored",
      spark.table("orders").count() == 12, str(spark.table("orders").count()))

try:
    f.fuzz(QUERY, seeds, iterations=1, strategy="nope")
    check("unknown strategy is rejected", False, "no error raised")
except Exception as e:
    check("unknown strategy is rejected", "natural" in str(e), str(e)[:120])

print("\n%d checks passed" % len(passed))
spark.stop()
