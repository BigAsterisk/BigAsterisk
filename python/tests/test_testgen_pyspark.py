# End-to-end PySpark test for systematic test-input generation.
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
orders.createOrReplaceTempView("orders")

g = bigasterisk.testgen(spark)
seeds = {"orders": orders}

suite = g.generate("SELECT cid FROM orders WHERE amount > 100", seeds)
check("both sides of the filter are generated", len(suite.cases) == 2, str(len(suite.cases)))
check("at least one case is verified", len(suite.verified) > 0, repr(suite))
check("branches are counted", suite.total_branches > 0, str(suite.total_branches))
check("coverage is a fraction", 0.0 < suite.coverage <= 1.0, str(suite.coverage))
check("suite repr is readable", "TestSuite" in repr(suite), repr(suite))

taking = [c for c in suite.cases if not c.path.startswith("NOT")][0]
check("the taking case is verified", taking.verified is True, taking.note)
check("verified cases say so", taking.note == "verified", taking.note)
check("generated rows are reported", len(taking.tables["orders"]) > 0, str(taking.tables))
check("case repr is readable", "ok" in repr(taking), repr(taking))

# without naturalness the solver picks the boundary: > 100 on an INT means 101
synthetic = g.generate("SELECT cid FROM orders WHERE amount > 100", seeds,
                       natural=False, rows_per_path=1)
boundary = [c for c in synthetic.cases if not c.path.startswith("NOT")][0]
check("the solver derives the boundary value",
      "101" in boundary.tables["orders"][0], str(boundary.tables["orders"]))

impossible = g.generate("SELECT cid FROM orders WHERE amount > 200 AND amount < 100", seeds)
check("an unsatisfiable path is reported",
      any(c.note == "unsatisfiable" for c in impossible.cases),
      str([c.note for c in impossible.cases]))

none = g.generate("SELECT cid, amount FROM orders", seeds)
check("a query with no branches yields no cases", none.cases == [], str(none.cases))

check("the caller's views are restored", spark.table("orders").count() == 12)

print("\n%d checks passed" % len(passed))
spark.stop()
