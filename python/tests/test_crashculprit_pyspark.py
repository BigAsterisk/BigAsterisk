# End-to-end PySpark test for crash-culprit determination.
#
# Run through python/tests/run.sh, which builds the jars, zips the package and
# submits this file.

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

import bigasterisk

passed = []


def check(name, cond, detail=""):
    if cond:
        passed.append(name)
        print("PASS  %s" % name)
    else:
        print("FAIL  %s  %s" % (name, detail))
        sys.exit(1)


spark = bigasterisk.configure(
    SparkSession.builder.config("spark.task.maxFailures", "1")).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

ROOT = os.environ.get("BIGASTERISK_HOME", ".")
DATA = os.path.join(ROOT, "modules/spark4/src/test/resources")
orders = spark.read.schema("oid STRING, cid STRING, amount INT") \
              .csv(os.path.join(DATA, "orders_csv")).coalesce(1)


@udf(returnType=IntegerType())
def explodes_on_outlier(amount):
    if amount is not None and amount > 1000:
        raise ValueError("cannot handle %d" % amount)
    return amount


g = bigasterisk.crash_culprit(spark)
g.clear()

# A JVM-side failure on exactly one record: dividing by (amount - 99999) is a division
# by zero for the outlier alone, and ANSI mode makes that an error rather than a null.
spark.conf.set("spark.sql.ansi.enabled", "true")
guard = g.guard(orders)
try:
    guard.df.selectExpr("oid", "100 DIV (amount - 99999) AS boom").collect()
    check("the query failed as intended", False, "no exception raised")
except Exception:
    pass

culprit = guard.culprit
check("a culprit is reported", culprit is not None)
check("it is the outlier record", culprit.row["amount"] == 99999, str(culprit.row))
check("the record keeps every column",
      set(culprit.row.keys()) == {"oid", "cid", "amount"}, str(culprit.row))
check("its position is reported",
      culprit.partition_id == 0 and culprit.record_index == 7,
      "%s/%s" % (culprit.partition_id, culprit.record_index))
check("the error is reported", "DIVIDE_BY_ZERO" in culprit.error, culprit.error)
check("repr is readable", "partition 0" in repr(culprit), repr(culprit))

guard.reset()
check("reset clears the report", guard.culprit is None)

# A batched Python UDF takes a whole batch to the worker before any of it can fail, so
# the guard has already emitted the whole batch by then and cannot name the record.
# Pinned here so the documented limitation stays true rather than drifting.
batched = g.guard(orders)
try:
    batched.df.withColumn("boom", explodes_on_outlier(col("amount"))).collect()
except Exception:
    pass
check("a batched Python UDF defeats record attribution",
      batched.culprit is not None and batched.culprit.row["amount"] != 99999,
      str(batched.culprit))

spark.conf.set("spark.sql.ansi.enabled", "false")

clean = g.guard(orders)
clean.df.collect()
check("a query that succeeds reports no culprit", clean.culprit is None)
check("guard repr says so", "no failure" in repr(clean), repr(clean))

check("guards are registered", len(g.active()) >= 2)
g.clear()
check("clear empties the registry", g.active() == [])

print("\n%d checks passed" % len(passed))
spark.stop()
