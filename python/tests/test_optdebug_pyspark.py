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

narrowed = o.localize(FAULTY, faulty_where="total < 0", base_table="orders")
check("minimisation is reported", narrowed.minimised is True, repr(narrowed))
check("the failing population is narrowed to the culprit",
      narrowed.minimised_from == 4 and narrowed.failing_witnesses == 1,
      "%s -> %s" % (narrowed.minimised_from, narrowed.failing_witnesses))
check("the narrowed prime is the faulty branch",
      narrowed.prime.is_branch and abs(narrowed.prime.score - 1.0) < 1e-9,
      repr(narrowed.prime))

# without narrowing Ochiai ranks the all-touching aggregation first; with it, the branch
narrowed_ochiai = o.localize(FAULTY, faulty_where="total < 0",
                             formula="ochiai", base_table="orders")
check("minimisation makes ochiai viable", narrowed_ochiai.prime.is_branch is True,
      repr(narrowed_ochiai.prime))

check("the base table is restored", spark.table("orders").count() == 12)

# The same computation as a DataFrame pipeline rather than a SQL string. Minimising it
# means substituting into its plan, since it is already bound to `orders`; the answer
# should not depend on which way the query was written.
from pyspark.sql.functions import col, sum as sum_, when          # noqa: E402

pipeline = (spark.table("orders")
            .groupBy(col("cid"))
            .agg(sum_(when(col("amount") > 1000, -col("amount"))
                      .otherwise(col("amount"))).alias("total")))

from_frame = o.localize(pipeline, faulty_where="total < 0", base_table="orders")
check("a DataFrame pipeline minimises too", from_frame.minimised is True,
      repr(from_frame))
check("the DataFrame form narrows to the same culprit",
      from_frame.failing_witnesses == narrowed.failing_witnesses,
      "%s vs %s" % (from_frame.failing_witnesses, narrowed.failing_witnesses))
check("the DataFrame form finds the same faulty branch",
      from_frame.prime.is_branch and "1000" in (from_frame.prime.branch or ""),
      repr(from_frame.prime))
check("substituting a DataFrame leaves the session alone",
      spark.table("orders").count() == 12)

try:
    o.localize(FAULTY, faulty_where="total < 0", formula="nope")
    check("unknown formula is rejected", False, "no error raised")
except Exception as e:
    check("unknown formula is rejected", "tarantula" in str(e), str(e)[:120])

print("\n%d checks passed" % len(passed))
spark.stop()
