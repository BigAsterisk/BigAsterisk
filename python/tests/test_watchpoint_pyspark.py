# End-to-end PySpark test for on-demand watchpoints.
#
# Run through python/tests/run.sh, which builds the jars, zips the package and
# submits this file.

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

w = bigasterisk.watchpoints(spark)
orders = spark.table("orders")

# amounts: 420 250 80 310 190 95 60 99999 75 110 205 380
wp = w.watch(orders, col("amount") > 300)
wp.df.collect()
check("counts matching rows", wp.hits == 4, "got %d" % wp.hits)

amounts = sorted(r["amount"] for r in wp.captured)
check("captures the matching rows", amounts == [310, 380, 420, 99999], str(amounts))
check("not truncated within capacity", wp.truncated is False)

# pass-through
check("instrumented df yields every row", wp.df.count() == 12)

# capacity
wp.reset()
bounded = w.watch(orders, col("amount") > 100, capacity=3)
bounded.df.collect()
check("capacity bounds captured but not hits",
      bounded.hits == 8 and len(bounded.captured) == 3,
      "hits=%d captured=%d" % (bounded.hits, len(bounded.captured)))
check("truncated is reported", bounded.truncated is True)

# survives a shuffle, and keeps every column of the watched DataFrame
shuffled = w.watch(orders, col("amount") > 300)
shuffled.df.groupBy("cid").sum("amount").collect()
check("observations survive a shuffle", shuffled.hits == 4, "got %d" % shuffled.hits)
check("captured rows keep all watched columns",
      all(set(r.keys()) == {"oid", "cid", "amount"} for r in shuffled.captured),
      str(shuffled.captured[:1]))

# reset
shuffled.reset()
check("reset clears observations",
      shuffled.hits == 0 and shuffled.captured == [])

# registry
w.clear()
a = w.watch(orders, col("amount") > 300)
b = w.watch(orders, col("amount") > 100)
ids = [x.id for x in w.active()]
check("registry lists live watchpoints", a.id in ids and b.id in ids and a.id != b.id, str(ids))
w.clear()
check("clear empties the registry", w.active() == [])

check("condition is readable", "amount" in a.condition, a.condition)
check("repr is readable", "Watchpoint" in repr(a), repr(a))

print("\n%d checks passed" % len(passed))
spark.stop()
