# End-to-end PySpark test for simulated breakpoints.
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

b = bigasterisk.breakpoints(spark)
b.clear()

bp = b.breakpoint(orders.filter("amount > 100"))
check("the shape is known without computing", bp.schema.fieldNames() == ["oid", "cid", "amount"])
check("nothing is pinned yet", bp.is_materialized is False)

state = bp.state()
check("state is the records flowing past", len(state) == 8, str(len(state)))
check("every record satisfies the prefix", all(r["amount"] > 100 for r in state))
check("count is exact", bp.count() == 8, str(bp.count()))
check("state respects the limit", len(bp.state(limit=3)) == 3)

# the query runs through the breakpoint unchanged
downstream = {(r["cid"], r["sum(amount)"]) for r in bp.df.groupBy("cid").sum("amount").collect()}
expected = {(r["cid"], r["sum(amount)"])
            for r in orders.filter("amount > 100").groupBy("cid").sum("amount").collect()}
check("the query is unchanged by the breakpoint", downstream == expected)

bp.materialize()
check("materialize pins the state", bp.is_materialized is True)

# resume from the breakpoint with a corrected step
whole = b.breakpoint(orders)
whole.materialize()
totals = {r["cid"]: r["sum(amount)"] for r in
          whole.resume_with(
              lambda d: d.filter("amount <= 1000").groupBy("cid").sum("amount")).collect()}
check("resuming with a corrected step works", totals["c2"] == 645, str(totals))

check("repr is readable", "Breakpoint" in repr(bp), repr(bp))
check("breakpoints are registered", len(b.active()) >= 2)

b.clear()
check("clear empties the registry", b.active() == [])
check("clear releases what was pinned", bp.is_materialized is False)

try:
    bp.state(limit=-1)
    check("a negative limit is rejected", False, "no error raised")
except ValueError:
    check("a negative limit is rejected", True)

print("\n%d checks passed" % len(passed))
spark.stop()
