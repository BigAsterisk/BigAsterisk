# The PySpark front end, running against the cluster.
#
#   scripts/cluster.sh run pydemo
#
# Everything here goes through the Python API rather than Scala, and the last section
# reads inside a Python UDF — the analysis that lets test generation, operation isolation
# and influence see past a UDF boundary.

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

import bigasterisk

HOME = os.environ.get("BIGASTERISK_HOME", "/opt/bigasterisk")
DATA = os.path.join(HOME, "examples", "data")

spark = bigasterisk.configure(SparkSession.builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\n== connected to %s" % spark.sparkContext.master)
print("   executors: %d" % len(
    [e for e in spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()]))

orders = spark.read.schema("oid STRING, cid STRING, amount INT") \
              .csv(os.path.join(DATA, "orders.txt"))
customers = spark.read.schema("cid STRING, name STRING") \
                 .csv(os.path.join(DATA, "customers.txt"))
orders.createOrReplaceTempView("orders")
customers.createOrReplaceTempView("customers")

FAULTY = ("SELECT cid, SUM(CASE WHEN amount > 1000 THEN -amount ELSE amount END) AS total "
          "FROM orders GROUP BY cid")

# ---------------------------------------------------------------------------
print("\n== DeSQL — the query, decomposed")
for step in bigasterisk.desql(spark).decompose(spark.sql(FAULTY)):
    print("   [%d] %s" % (step.id, step.operator))

# ---------------------------------------------------------------------------
print("\n== Titian — the records behind a wrong result")
lineage = bigasterisk.lineage(spark)
lineage.enable_capture()
try:
    # the same DataFrame instance throughout: lineage ids belong to the execution that
    # produced them, so tracing a freshly built query would have nothing to trace
    faulty = spark.sql(FAULTY)
    rows = lineage.collect_with_lineage(faulty)
    wrong = [(row, ids) for row, ids in rows if row["total"] < 0]
    # str() first: a Row is a tuple subclass, and %-formatting would unpack it
    print("   wrong output: %s" % str(wrong[0][0] if wrong else "none"))
    cursor = lineage.trace(faulty, [wrong[0][1]])
    while not cursor.at_scan:
        cursor = cursor.go_back()
    print("   %d source records produced it" % len(cursor.show()))
finally:
    lineage.disable_capture()

# ---------------------------------------------------------------------------
print("\n== FlowDebug — which of those records mattered")
for influence in bigasterisk.influence(spark).influencers(FAULTY, "total < 0")[:2]:
    print("   %s" % influence)

# ---------------------------------------------------------------------------
print("\n== BigTest — an input per path")
suite = bigasterisk.testgen(spark).generate(
    "SELECT cid FROM orders WHERE amount > 100", {"orders": orders}, rows_per_path=1)
for case in suite.cases:
    print("   %s" % case)

# ---------------------------------------------------------------------------
print("\n== Fuzzing — three strategies on one joined query")
QUERY = ("SELECT c.name, SUM(o.amount) AS total FROM orders o "
         "JOIN customers c ON o.cid = c.cid WHERE o.amount > 100 GROUP BY c.name")
seeds = {"orders": orders, "customers": customers}
for strategy in ("random", "natural", "co-dependent"):
    result = bigasterisk.fuzz(spark).fuzz(QUERY, seeds, iterations=20, strategy=strategy, seed=1)
    print("   %-13s coverage %.0f%%  empty results %d/%d"
          % (strategy, result.coverage * 100, result.empty_results, result.iterations))

# ---------------------------------------------------------------------------
print("\n== Reading inside a Python UDF")


def classify(amount):
    if amount > 1000:
        return "high"
    elif amount > 100:
        return "medium"
    return "low"


spark.udf.register("classify", classify, StringType())
profile = bigasterisk.udf.register(spark, classify)
print("   %r" % profile)
for condition, params in profile.branches:
    print("   branch: %s" % condition)

# With the profile registered, a condition on the UDF's *result* becomes conditions on
# its *argument*, so the solver can build an input for each path through it.
udf_suite = bigasterisk.testgen(spark).generate(
    "SELECT oid FROM orders WHERE classify(amount) = 'high'", {"orders": orders}, seed=1)
verified = [c for c in udf_suite.cases if c.verified]
print("   %d of %d generated inputs verified" % (len(verified), len(udf_suite.cases)))
for case in verified[:2]:
    print("   %s" % case)

print("\nPYSPARK DEMO OK")
spark.stop()
