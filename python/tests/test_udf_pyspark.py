# End-to-end PySpark test for seeing inside a Python UDF.
#
# The three techniques that stopped at a UDF boundary — symbolic test generation,
# operation-level fault localisation, and taint-refined influence — are checked here
# against real Python UDFs running in real queries.
#
# Run through python/tests/run.sh, which builds the jars, zips the package and
# submits this file.

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType

import bigasterisk

passed = []


def check(name, cond, detail=""):
    if cond:
        passed.append(name)
        print("PASS  %s" % name)
    else:
        print("FAIL  %s  %s" % (name, detail))
        sys.exit(1)


# --- the UDFs under test ---------------------------------------------------

def classify(amount):
    if amount > 1000:
        return "high"
    elif amount > 100:
        return "medium"
    return "low"


def band(amount):
    if amount > 1000:
        return -amount          # the planted fault: an outlier flips sign
    return amount


def score(amount, note):
    return amount * 2           # `note` is passed and never read


spark = bigasterisk.configure(SparkSession.builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

ROOT = os.environ.get("BIGASTERISK_HOME", ".")
DATA = os.path.join(ROOT, "modules/spark4/src/test/resources")
orders = spark.read.schema("oid STRING, cid STRING, amount INT") \
              .csv(os.path.join(DATA, "orders_csv"))
orders.createOrReplaceTempView("orders")

spark.udf.register("classify", classify, StringType())
spark.udf.register("band", band, IntegerType())
spark.udf.register("score", score, IntegerType())

# --- registering a profile -------------------------------------------------

profile = bigasterisk.udf.register(spark, classify)
check("the profile reads the function", profile.solvable, repr(profile))
check("the profile is visible to the JVM",
      "classify" in bigasterisk.udf.registered(spark),
      bigasterisk.udf.registered(spark))

# --- BigTest: solving through the UDF --------------------------------------

QUERY = "SELECT oid FROM orders WHERE classify(amount) = 'high'"
testgen = bigasterisk.testgen(spark)

suite = testgen.generate(QUERY, {"orders": orders}, seed=1)
verified = [case for case in suite.cases if case.verified]
check("a test is generated for a path through the UDF", len(verified) > 0,
      "\n".join(repr(c) for c in suite.cases))
check("the generated test drives the branch inside the UDF",
      any("amount" in case.path and "1000" in case.path for case in verified),
      [c.path for c in verified])

# the rows it built really do make the query return something
reached = [case for case in verified if "NOT" not in case.path.split(" AND ")[0]]
check("the input built for the high branch is a real input", len(reached) > 0,
      [c.path for c in verified])

# without a profile the same query is a black box, and nothing can be solved
bigasterisk.udf.unregister(spark, "classify")
blind = testgen.generate(QUERY, {"orders": orders}, seed=1)
check("without a profile the UDF cannot be solved through",
      all(not case.verified for case in blind.cases),
      "\n".join(repr(c) for c in blind.cases))
check("and it says why rather than claiming coverage",
      any("solver" in case.note or "unsupported" in case.note for case in blind.cases),
      [c.note for c in blind.cases])

bigasterisk.udf.register(spark, classify)

# --- OptDebug: ranking a branch inside the UDF -----------------------------

bigasterisk.udf.register(spark, band)
faulty = spark.sql("SELECT oid, band(amount) AS value FROM orders")

result = bigasterisk.optdebug(spark).localize(faulty, "value < 0")
branches = [op for op in result.ranked if op.branch]
check("a branch inside the UDF is scored as an operation",
      any("amount" in (op.branch or "") and "1000" in (op.branch or "")
          for op in branches),
      "\n".join(repr(op) for op in result.ranked))

top = result.ranked[0]
check("the faulty branch is the most suspicious operation",
      top.branch is not None and "1000" in top.branch, repr(top))

# --- FlowDebug: taint narrows which columns mattered -----------------------

bigasterisk.udf.register(spark, score)
ranked = bigasterisk.influence(spark).influencers(
    "SELECT cid, MAX(score(amount, oid)) AS peak FROM orders GROUP BY cid",
    faulty_where="peak > 1000")

check("something was ranked", len(ranked) > 0)
best = ranked[0]
check("influence names only the column that reached the result",
      best.columns == {"amount"}, "%s %s" % (best.columns, repr(best)))
check("and reports that as a narrowing", best.narrowed, repr(best))

# the same query without a profile implicates every column the call reads
bigasterisk.udf.unregister(spark, "score")
blind = bigasterisk.influence(spark).influencers(
    "SELECT cid, MAX(score(amount, oid)) AS peak FROM orders GROUP BY cid",
    faulty_where="peak > 1000")
check("without a profile both arguments stay implicated",
      blind[0].columns == {"amount", "oid"}, blind[0].columns)

# --- a function the analysis cannot fully read -----------------------------

def partly_readable(text):
    if text.encode("utf8") == b"x":
        return "odd"
    return "even"


partial = bigasterisk.udf.analyze(partly_readable)
check("a partly readable function still produces a profile", not partial.complete)
check("and it names what it could not read", len(partial.unsupported) == 1,
      partial.unsupported)
check("and is never solved through", not partial.solvable)

print("\n%d checks passed" % len(passed))
