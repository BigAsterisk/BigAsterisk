# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""BigAsterisk — a unified debugging and testing platform for Apache Spark.

The PySpark front end mirrors the Scala API: configure a session so the binding for
your Spark version installs itself, then reach the tools through it.

    import bigasterisk
    from pyspark.sql import SparkSession

    spark = bigasterisk.configure(SparkSession.builder.master("local[*]")).getOrCreate()

    lin = bigasterisk.lineage(spark)
    lin.enable_capture()

    df = spark.sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category")
    rows = lin.collect_with_lineage(df)
    print(lin.trace(df, [rows[0][1]]).go_back().show(full=True))
    lin.release_lineage(df)

Requires the BigAsterisk jars on the Spark classpath (``--jars``) and this package on
the Python path (``--py-files``).
"""

from bigasterisk.lineage import Lineage, TraceCursor
from bigasterisk.bigsift import BigSift, BigSiftResult, ddmin
from bigasterisk.desql import DeSql, QueryStep
from bigasterisk.watchpoint import Watchpoint, Watchpoints
from bigasterisk.vega import Vega, VegaRun
from bigasterisk.optdebug import OptDebug, OptDebugResult, SuspiciousOperation

__all__ = [
    "configure",
    "lineage",
    "bindings",
    "Lineage",
    "TraceCursor",
    "BigSift",
    "BigSiftResult",
    "ddmin",
    "desql",
    "DeSql",
    "QueryStep",
    "watchpoints",
    "Watchpoint",
    "Watchpoints",
    "vega",
    "Vega",
    "VegaRun",
    "optdebug",
    "OptDebug",
    "OptDebugResult",
    "SuspiciousOperation",
]

#: The ``spark.sql.extensions`` entries the Spark 4 binding installs, mirroring
#: ``Spark4Binding.extensionClassNames``. Kept here rather than read from the JVM
#: because :func:`configure` runs before a gateway exists — Spark reads
#: ``spark.sql.extensions`` while building the session, so it cannot be set afterwards.
_SPARK4_EXTENSIONS = (
    "org.apache.spark.sql.lineage.TitianSQLExtension",
    "org.apache.spark.sql.watchpoint.WatchpointExtension",
)


def configure(builder, extensions=_SPARK4_EXTENSIONS):
    """Add the BigAsterisk SQL extensions to a ``SparkSession.Builder``.

    Spark reads ``spark.sql.extensions`` when the session is built, so this must be
    called before ``getOrCreate()``. Any extensions already configured on the builder
    are preserved — this appends rather than replaces.

    Returns the builder, so it chains::

        spark = bigasterisk.configure(SparkSession.builder.master("local[*]")).getOrCreate()
    """
    if isinstance(extensions, str):
        extensions = (extensions,)
    # `_options` is PySpark-internal; fall back to appending blind if it ever moves,
    # which is still correct for the common case of nothing else being configured.
    options = getattr(builder, "_options", None)
    existing = options.get("spark.sql.extensions", "") if isinstance(options, dict) else ""
    entries = [e.strip() for e in str(existing).split(",") if e.strip()]
    for extension in extensions:
        if extension not in entries:
            entries.append(extension)
    return builder.config("spark.sql.extensions", ",".join(entries))



def bindings(spark):
    """Names of the BigAsterisk Spark bindings visible on the JVM classpath."""
    jb = spark._jvm.org.bigasterisk.api.BigAsterisk.bindings()
    return [jb.apply(i).name() for i in range(jb.size())]


def lineage(spark):
    """Record-level data provenance for ``spark``.

    Resolves the binding for the running Spark version through the JVM entry point,
    so Python and Scala agree on which engine is in use.

    :raises RuntimeError: if no binding matches, or the session was built without
        :func:`configure`. The JVM's message explains which and how to fix it.
    """
    return Lineage(spark)


def desql(spark):
    """Step-through SQL debugging for ``spark``.

    Decompose a query into its constituent parts and inspect the intermediate data at
    each one::

        for step in bigasterisk.desql(spark).decompose(df):
            print(step)
            step.data.show()
    """
    return DeSql(spark)


def watchpoints(spark):
    """On-demand watchpoints for ``spark``.

    Guard the intermediate data of a query and see which records match, without
    collecting the intermediate dataset::

        wp = bigasterisk.watchpoints(spark).watch(orders, col("amount") > 10000)
        wp.df.groupBy("cid").sum("amount").collect()
        print(wp.hits, wp.captured)
    """
    return Watchpoints(spark)


def vega(spark):
    """Incremental re-execution for ``spark``.

    Successive revisions of a query start from the deepest point they still share::

        v = bigasterisk.vega(spark)
        v.run("SELECT cid, amount FROM orders WHERE amount > 100").df.collect()
        r = v.run("SELECT cid, SUM(amount) FROM orders WHERE amount > 100 GROUP BY cid")
        print(r.reused)
    """
    return Vega(spark)


def optdebug(spark):
    """Fault-inducing operation isolation for ``spark``.

    Rank a query's operations by how responsible each looks for a wrong result::

        result = bigasterisk.optdebug(spark).localize(query, faulty_where="total < 0")
        print(result.prime)
    """
    return OptDebug(spark)
