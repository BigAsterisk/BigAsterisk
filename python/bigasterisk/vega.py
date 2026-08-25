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


"""Incremental re-execution across successive revisions of a query.

Exploratory analysis is a sequence of near-identical queries: run one, look at the
answer, change a projection or add a grouping, run it again. Each run normally starts
from nothing even though most of the work is the same as last time. Vega materializes
the reusable parts as a query runs, so the next revision starts from the deepest point
the two still share.

    import bigasterisk

    vega = bigasterisk.vega(spark)

    v1 = vega.run("SELECT cid, amount FROM orders WHERE amount > 100")
    v1.df.collect()

    v2 = vega.run("SELECT cid, SUM(amount) AS total FROM orders "
                  "WHERE amount > 100 GROUP BY cid")
    print(v2.reused)        # the scan and the filter, served from v1
    v2.df.collect()

From *Optimizing Interactive Development of Data-Intensive Applications* (SoCC 2016).
"""


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


def _strings(jseq):
    return [jseq.apply(i) for i in range(jseq.size())]


class VegaRun:
    """The outcome of preparing one query revision for execution."""

    def __init__(self, jrun, spark):
        self._j = jrun
        self._spark = spark

    @property
    def df(self):
        """The DataFrame to execute. Semantically identical to the one passed in."""
        return _wrap_dataframe(self._j.df(), self._spark)

    @property
    def reused(self):
        """Parts of this query served from a previous revision, deepest first."""
        return _strings(self._j.reused())

    @property
    def materialized(self):
        """Parts materialized during this run, for the next revision."""
        return _strings(self._j.materialized())

    @property
    def steps(self):
        """How many parts this query decomposes into."""
        return self._j.steps()

    @property
    def rewritten(self):
        """Whether filters were moved later in the plan so more work stays reusable.

        A normalisation, applied to every revision so that successive ones are in the
        same shape. It preserves the answer, and costs nothing at execution time because
        Catalyst pushes filters back down while optimising.
        """
        return self._j.rewritten()

    @property
    def reuse_ratio(self):
        """Fraction of this query's parts that came from a previous revision."""
        return self._j.reuseRatio()

    def __repr__(self):
        return "VegaRun(steps=%d, reused=%d, materialized=%d%s)" % (
            self.steps, len(self.reused), len(self.materialized),
            ", rewritten" if self.rewritten else "")


class Vega:
    """Incremental re-execution for a SparkSession."""

    def __init__(self, spark):
        self._spark = spark
        self._support = spark._jvm.org.bigasterisk.api.BigAsterisk.vega(
            spark._jsparkSession)

    def run(self, df):
        """Prepare ``df`` for execution, reusing and materializing what it can.

        Accepts either a DataFrame or a query string. Materialization costs time and
        memory on the run that performs it and pays for itself on the next revision.
        """
        if isinstance(df, str):
            df = self._spark.sql(df)
        return VegaRun(self._support.run(df._jdf), self._spark)

    @property
    def materialized(self):
        """Everything currently materialized, across all revisions."""
        return _strings(self._support.materialized())

    @property
    def max_materialized(self):
        """The most query parts held materialized at once."""
        return self._support.maxMaterialized()

    def clear(self):
        """Release every materialized result."""
        self._support.clear()
