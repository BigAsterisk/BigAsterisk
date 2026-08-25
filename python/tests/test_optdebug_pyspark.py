# End-to-end PySpark test for fault-inducing operation isolation.
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

# a planted fault: amounts over 1000 are negated, which only the one outlier hits
FAULTY = ("SELECT cid, SUM(CASE WHEN amount > 1000 THEN -amount ELSE amount END) AS total "
          "FROM orders GROUP BY cid")

o = bigasterisk.optdebug(spark)
result = o.localize(FAULTY, faulty_where="total < 0")

check("something was ranked", len(result.ranked) > 0)
check("formula is reported", result.formula == "tarantula", result.formula)

top = result.prime
check("the faulty branch is ranked first", top.is_branch, repr(top))
check("the branch names the faulty condition",
      "1000" in top.branch, str(top.branch))
check("the branch is fully suspicious", abs(top.score - 1.0) < 1e-9, str(top.score))
check("the branch covers no passing witnesses", top.passing_witnesses == 0,
      str(top.passing_witnesses))

check("witness populations are reported",
      result.failing_witnesses == 4 and result.passing_witnesses == 8,
      "%d/%d" % (result.failing_witnesses, result.passing_witnesses))

neutral = [op for op in result.ranked if not op.is_branch and op.operator == "Aggregate"]
check("an all-touching operator scores neutrally",
      neutral and abs(neutral[0].score - 0.5) < 1e-9,
      str([op.score for op in neutral]))

ochiai = o.localize(FAULTY, faulty_where="total < 0", formula="ochiai")
check("ochiai is selectable", ochiai.formula == "ochiai", ochiai.formula)

check("operation repr is readable", "Aggregate" in repr(top), repr(top))
check("result repr is readable", "OptDebugResult" in repr(result), repr(result))

try:
    o.localize(FAULTY, faulty_where="total < 0", formula="nope")
    check("unknown formula is rejected", False, "no error raised")
except Exception as e:
    check("unknown formula is rejected", "tarantula" in str(e), str(e)[:120])

print("\n%d checks passed" % len(passed))
spark.stop()
