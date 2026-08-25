# End-to-end PySpark test for influence-based provenance.
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

inf = bigasterisk.influence(spark)

# c2 holds the outlier: 250, 190, 99999, 205. Provenance would return all four.
ranked = inf.influencers(
    "SELECT cid, MAX(amount) AS peak FROM orders GROUP BY cid", faulty_where="peak > 1000")

check("something was ranked", len(ranked) > 0)
top = ranked[0]
check("only the maximum influences", abs(top.score - 1.0) < 1e-9, str(top.score))
check("it is the outlier record", top.row["amount"] == 99999, str(top.row))
check("the reason is explained", "maximum" in top.reason, top.reason)
check("the other witnesses score zero",
      all(r.score == 0.0 for r in ranked[1:]), str([r.score for r in ranked[1:]]))

sums = inf.influencers(
    "SELECT cid, SUM(amount) AS total FROM orders GROUP BY cid", faulty_where="total > 50000")
check("sum influence is the contribution share", sums[0].score > 0.99, str(sums[0].score))
check("shares of a group sum to one",
      abs(sum(r.score for r in sums) - 1.0) < 1e-6, str(sum(r.score for r in sums)))
check("sum reason mentions contribution", "contribution" in sums[0].reason, sums[0].reason)

counts = inf.influencers(
    "SELECT cid, COUNT(*) AS n FROM orders GROUP BY cid", faulty_where="cid = 'c3'")
check("count weights every record equally",
      len({round(r.score, 9) for r in counts}) == 1, str([r.score for r in counts]))

bounded = inf.influencers(
    "SELECT cid, SUM(amount) AS total FROM orders GROUP BY cid",
    faulty_where="total > 50000", top_k=2)
check("top_k bounds the result", len(bounded) == 2, str(len(bounded)))

check("records carry their own columns",
      set(top.row.keys()) == {"oid", "cid", "amount"}, str(top.row))
check("repr is readable", "only the maximum" in repr(top), repr(top))

print("\n%d checks passed" % len(passed))
spark.stop()
