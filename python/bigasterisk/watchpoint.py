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


"""On-demand watchpoints over the intermediate data of a Spark SQL query.

A watchpoint is a guard on records flowing through a point in a query: matching rows
are counted and sampled back to the driver, without collecting the intermediate
dataset. This is BigDebug's watchpoint primitive (ICSE 2016).

    import bigasterisk
    from pyspark.sql.functions import col

    orders = spark.table("orders")
    wp = bigasterisk.watchpoints(spark).watch(orders, col("amount") > 10000)

    # build the rest of the query on the instrumented DataFrame
    wp.df.groupBy("cid").sum("amount").collect()

    print("%d suspicious rows" % wp.hits)
    for row in wp.captured:
        print(row)
"""

import json


def _wrap_dataframe(jdf, spark):
    """Wrap a JVM DataFrame as a PySpark DataFrame.

    PySpark has moved this constructor between releases, so try the session-based form
    first and fall back to the older SQLContext form.
    """
    from pyspark.sql import DataFrame

    try:
        return DataFrame(jdf, spark)
    except TypeError:
        return DataFrame(jdf, spark._wrapped)


class Watchpoint:
    """A guard on the rows flowing out of a DataFrame."""

    def __init__(self, jwatchpoint, spark):
        self._j = jwatchpoint
        self._spark = spark

    @property
    def id(self):
        """Identifier, unique in the session. Names the accumulator in the Spark UI."""
        return self._j.id()

    @property
    def condition(self):
        """The guard, as SQL text."""
        return self._j.condition()

    @property
    def df(self):
        """The instrumented DataFrame.

        Build the rest of your query on this, not on the DataFrame you passed to
        :meth:`Watchpoints.watch`, or nothing will be observed.
        """
        return _wrap_dataframe(self._j.df(), self._spark)

    @property
    def hits(self):
        """How many rows matched — counted in full, even beyond ``capacity``."""
        return self._j.hits()

    @property
    def capacity(self):
        """The most matching rows this watchpoint will bring back to the driver."""
        return self._j.capacity()

    @property
    def captured(self):
        """The matching rows retained, as a list of dicts."""
        return [json.loads(s) for s in self._j.capturedJson()]

    @property
    def truncated(self):
        """True when more rows matched than were kept, so ``captured`` is a sample."""
        return self._j.truncated()

    def reset(self):
        """Discard what has been observed, so the watchpoint can be reused for a new run."""
        self._j.reset()

    def __repr__(self):
        return "Watchpoint(%s, condition=%s, hits=%d%s)" % (
            self.id, self.condition, self.hits, ", truncated" if self.truncated else "")


class Watchpoints:
    """Watchpoint creation and registry for a SparkSession."""

    def __init__(self, spark):
        self._spark = spark
        self._support = spark._jvm.org.bigasterisk.api.BigAsterisk.watchpoints(
            spark._jsparkSession)

    def watch(self, df, condition, capacity=1000):
        """Place a watchpoint on the rows flowing out of ``df``.

        ``condition`` is a PySpark ``Column``. Evaluation happens on the executors and
        is fused into Spark's generated code, so an unmatched row costs one predicate
        and moves no data.
        """
        return Watchpoint(
            self._support.watch(df._jdf, condition._jc, int(capacity)), self._spark)

    def active(self):
        """Every watchpoint created and not yet cleared."""
        j = self._support.active()
        return [Watchpoint(j.apply(i), self._spark) for i in range(j.size())]

    def clear(self):
        """Forget every watchpoint. Already-instrumented DataFrames keep working."""
        self._support.clear()
