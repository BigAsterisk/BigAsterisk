# End-to-end PySpark test for the BigAsterisk tab in the Spark UI.
#
# The tab is the interactive half of BigDebug, and its whole selling point is that it
# costs no code: the one call that installs the tools installs the tab. That claim is
# what this checks — including that the pages are actually served and actually carry the
# live state, since a tab that renders an empty shell would satisfy any weaker test.
#
# Run through python/tests/run.sh.

import html
import os
import re
import sys
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, udf
from pyspark.sql.types import StringType

import bigasterisk

passed = []


def check(name, cond, detail=""):
    if cond:
        passed.append(name)
        print("PASS  %s" % name)
    else:
        print("FAIL  %s  %s" % (name, detail))
        sys.exit(1)


# --------------------------------------------------------------------------
# the configuration, before any session exists
# --------------------------------------------------------------------------

class FakeBuilder(object):
    """Just enough of `SparkSession.Builder` to see what `configure` sets."""

    def __init__(self, **options):
        self._options = dict(options)

    def config(self, key, value):
        self._options[key] = value
        return self


plain = bigasterisk.configure(FakeBuilder())
check("configure registers the UI plugin",
      plain._options.get("spark.plugins") == "org.bigasterisk.spark4.BigAsteriskPlugin",
      repr(plain._options))
check("configure still registers the SQL extensions",
      "TitianSQLExtension" in plain._options.get("spark.sql.extensions", ""),
      repr(plain._options))

# a caller's own plugin must survive: dropping it to install a debugger would be a
# poor trade
mine = bigasterisk.configure(FakeBuilder(**{"spark.plugins": "com.example.MyPlugin"}))
check("a caller's own plugin is kept",
      mine._options["spark.plugins"].split(",")[0] == "com.example.MyPlugin",
      mine._options["spark.plugins"])
check("and ours is added alongside it",
      "org.bigasterisk.spark4.BigAsteriskPlugin" in mine._options["spark.plugins"],
      mine._options["spark.plugins"])

twice = bigasterisk.configure(bigasterisk.configure(FakeBuilder()))
check("configuring twice does not duplicate the plugin",
      twice._options["spark.plugins"].count("BigAsteriskPlugin") == 1,
      twice._options["spark.plugins"])

opted_out = bigasterisk.configure(FakeBuilder(), plugins=())
check("the UI can be declined", "spark.plugins" not in opted_out._options
      or opted_out._options["spark.plugins"] == "",
      repr(opted_out._options))

# --------------------------------------------------------------------------
# the tab itself, on a running application
# --------------------------------------------------------------------------

spark = (bigasterisk.configure(SparkSession.builder)
         .config("spark.ui.enabled", "true")).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

ROOT = os.environ.get("BIGASTERISK_HOME", ".")
DATA = os.path.join(ROOT, "modules/spark4/src/test/resources")
spark.read.schema("oid STRING, cid STRING, amount INT") \
     .csv(os.path.join(DATA, "orders_csv")).createOrReplaceTempView("orders")
orders = spark.table("orders")


@udf(returnType=StringType())
def band(amount):
    if amount < 500:
        return "small"
    return "large"


bigasterisk.udf.register(spark, band)

# a DataFrame pipeline with a watchpoint in front of it — the substitution a user makes
watched = bigasterisk.watchpoints(spark).watch(orders, col("amount") > 10000)
(watched.df
 .withColumn("band", band(col("amount")))
 .groupBy("band")
 .agg(count("*").alias("n"), avg(col("amount")).alias("mean"))).collect()

bigasterisk.breakpoints(spark).breakpoint(orders.filter(col("amount") > 100))

base = spark.sparkContext.uiWebUrl
check("the application has a UI", bool(base), repr(base))


def page(path):
    """One page of the tab, as text with the markup stripped and entities restored."""
    raw = urllib.request.urlopen(base + path, timeout=30).read().decode()
    return html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", raw)))


overview = page("/bigasterisk/")
check("the tab is served", "BigAsterisk — Overview" in overview, overview[:200])
check("it is in the navigation bar", "/bigasterisk/" in
      urllib.request.urlopen(base + "/jobs/", timeout=30).read().decode())
check("the overview counts what is live",
      "Watchpoints 1" in overview and "Breakpoints 1" in overview, overview[:600])

watchpoints = page("/bigasterisk/watchpoints/")
check("the watchpoint panel names the condition", "amount, 10000" in watchpoints,
      watchpoints[:400])
check("and shows the record it captured", "99999" in watchpoints, watchpoints[:400])

breakpoints = page("/bigasterisk/breakpoints/")
check("the breakpoint panel reports the schema",
      "oid, cid, amount" in breakpoints, breakpoints[:400])
check("and offers to regenerate the state",
      "regenerated on demand" in breakpoints, breakpoints[:400])

functions = page("/bigasterisk/functions/")
check("the functions panel shows what was read inside the UDF",
      "band(amount)" in functions and "amount < 500" in functions, functions[:400])

# Every panel reads state the tools already hold. If rendering the tab ran jobs of its
# own, this count would have moved.
before = len(spark.sparkContext.statusTracker().getJobIdsForGroup(None))
for path in ("/bigasterisk/", "/bigasterisk/watchpoints/", "/bigasterisk/crashes/",
             "/bigasterisk/latency/", "/bigasterisk/functions/"):
    page(path)
after = len(spark.sparkContext.statusTracker().getJobIdsForGroup(None))
check("rendering the tab runs no jobs", after == before, "%d -> %d" % (before, after))

print("\n%d checks passed" % len(passed))
spark.stop()
