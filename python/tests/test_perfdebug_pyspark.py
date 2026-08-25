# End-to-end PySpark test for computation-skew profiling.
#
# Run through python/tests/run.sh, which builds the jars, zips the package and
# submits this file.

import os
import sys
import time

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


spark = bigasterisk.configure(SparkSession.builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

ROOT = os.environ.get("BIGASTERISK_HOME", ".")
DATA = os.path.join(ROOT, "modules/spark4/src/test/resources")

orders = spark.read.schema("oid STRING, cid STRING, amount INT") \
              .csv(os.path.join(DATA, "orders_csv")).coalesce(1)


@udf(returnType=IntegerType())
def expensive_for_outlier(amount):
    if amount is not None and amount > 1000:
        time.sleep(0.06)
    return amount


p = bigasterisk.perfdebug(spark)
p.clear()

# an even workload first
plain = p.profile(orders, top_k=3)
plain.df.collect()
check("every record is counted but the first", plain.records == 11,
      "got %d" % plain.records)
check("mean is positive", plain.mean_nanos > 0)
check("retained records are bounded by top_k", len(plain.slowest) == 3,
      str(len(plain.slowest)))

# now a skewed one
skewed = p.profile(orders.withColumn("processed", expensive_for_outlier(col("amount"))),
                   top_k=3)
skewed.df.collect()

check("a plain pipeline attributes at record level", plain.record_level is True)

# A Python UDF runs in batches in a separate process, so its cost cannot be pinned to
# the record that caused it. The tool says so rather than reporting a confident wrong
# answer, and the totals stay exact.
check("a batched Python UDF is reported as not record level",
      skewed.record_level is False, str(skewed.record_level))
check("records are still counted across a batched UDF", skewed.records == 11,
      str(skewed.records))

# The whole batch is computed before the first output row appears, so its cost lands on
# the task's first record — which is deliberately not retained, since the interval
# before it spans pipeline start-up. Totals therefore understate across a batched
# operator. This is exactly why record_level is False.
top = skewed.slowest[0]
check("costs are still reported", top.millis >= 0.0, str(top.millis))
check("retained records are ordered most expensive first",
      [r.nanos for r in skewed.slowest] == sorted([r.nanos for r in skewed.slowest],
                                                  reverse=True))
check("record repr is readable", "ms" in repr(top), repr(top))
check("profile repr is readable", "PerfProfile" in repr(skewed), repr(skewed))

skewed.reset()
check("reset clears measurements", skewed.records == 0 and skewed.slowest == [])

check("profiles are registered", len(p.active()) >= 2)
p.clear()
check("clear empties the registry", p.active() == [])

print("\n%d checks passed" % len(passed))
spark.stop()
