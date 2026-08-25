# End-to-end PySpark test for BigSift automated fault isolation.
#
# Run through python/tests/run.sh, which builds the jars, zips the package and
# submits this file.

import os
import sys

from pyspark.sql import SparkSession

import bigasterisk
from bigasterisk import ddmin

passed = []


def check(name, cond, detail=""):
    if cond:
        passed.append(name)
        print("PASS  %s" % name)
    else:
        print("FAIL  %s  %s" % (name, detail))
        sys.exit(1)


# ---- pure ddmin ------------------------------------------------------------
check("ddmin isolates single element",
      ddmin(list(range(64)), lambda s: 7 in s) == [7])
check("ddmin sum scenario",
      ddmin([10, 20, -1000000, 15], lambda s: sum(s) < 0) == [-1000000])

spark = bigasterisk.configure(SparkSession.builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

base = os.path.dirname(os.path.abspath(__file__))
csv = os.path.join(base, "..", "..", "modules", "spark4", "src", "test", "resources", "bigsift_sales.csv")
spark.read.schema("category STRING, amount INT").csv(csv) \
    .createOrReplaceTempView("sales")

bs = bigasterisk.BigSift(spark)
result = bs.debug(
    "sales",
    "SELECT category, SUM(amount) AS total FROM sales GROUP BY category",
    lambda r: r["total"] < 0)

check("faulty output found",
      any(r["category"] == "electronics" for r in result.faulty_outputs),
      str(result.faulty_outputs))
check("provenance narrowed",
      3 <= result.provenance_size < 6, str(result.provenance_size))
check("isolates corrupt row",
      len(result.fault_inducing_rows) == 1 and
      result.fault_inducing_rows[0]["amount"] == -9999999,
      str(result.fault_inducing_rows))

# clean query -> no fault
clean = bs.debug(
    "sales",
    "SELECT category, SUM(amount) AS total FROM sales WHERE amount > 0 GROUP BY category",
    lambda r: r["total"] < 0)
check("clean data has no fault", clean.fault_inducing_rows == [])

print("ALL %d BIGSIFT PYSPARK TESTS PASSED" % len(passed))
spark.stop()
