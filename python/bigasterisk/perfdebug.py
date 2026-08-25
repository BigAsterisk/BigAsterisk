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


"""Performance debugging for computation skew.

Data skew — one key having far more rows than others — is visible in Spark's own
metrics. *Computation* skew is not: a small number of records can be far more expensive
to process than the rest, and no per-task metric says which ones. This measures cost at
record granularity so the expensive records can be named.

    import bigasterisk

    profile = bigasterisk.perfdebug(spark).profile(orders, top_k=10)
    profile.df.groupBy("cid").sum("amount").collect()

    print("skew: %.1fx the mean" % profile.skew)
    for record in profile.slowest:
        print(record)

From *PerfDebug: Performance Debugging of Computation Skew in Dataflow Systems*
(SoCC 2019).
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


class RecordCost:
    """One record and what it cost to produce."""

    def __init__(self, payload):
        self.nanos = payload.pop("__nanos")
        self.row = payload

    @property
    def millis(self):
        """The cost in milliseconds."""
        return self.nanos / 1e6

    def __repr__(self):
        return "%.3f ms  %r" % (self.millis, self.row)


class PerfProfile:
    """A profile of where a query's time went, record by record."""

    def __init__(self, jprofile, spark):
        self._j = jprofile
        self._spark = spark

    @property
    def df(self):
        """The instrumented DataFrame. Build the rest of the query on this."""
        return _wrap_dataframe(self._j.df(), self._spark)

    @property
    def slowest(self):
        """The most expensive records, most expensive first."""
        return [RecordCost(json.loads(s)) for s in self._j.slowestJson()]

    @property
    def records(self):
        """How many records passed the profiled point."""
        return self._j.records()

    @property
    def record_level(self):
        """Whether costs can be attributed to individual records.

        False when a batched Python or Arrow UDF sits below the profiled point: the
        whole batch is computed in one call to the Python worker, so its cost lands on
        whichever record triggered the batch rather than on the record that caused it.
        The totals stay exact either way.
        """
        return self._j.recordLevel()

    @property
    def total_nanos(self):
        """Total time attributed to those records."""
        return self._j.totalNanos()

    @property
    def mean_nanos(self):
        """Mean cost per record, in nanoseconds."""
        return self._j.meanNanos()

    @property
    def skew(self):
        """How far the costliest record sits above the mean, as a multiple.

        Near 1 means cost is spread evenly. A large value is the signature of
        computation skew.
        """
        return self._j.skew()

    def reset(self):
        """Discard measurements, so the profile can be reused for another run."""
        self._j.reset()

    def __repr__(self):
        return "PerfProfile(records=%d, mean=%.3f ms, skew=%.1fx)" % (
            self.records, self.mean_nanos / 1e6, self.skew)


class PerfDebug:
    """Computation-skew profiling for a SparkSession."""

    def __init__(self, spark):
        self._spark = spark
        self._support = spark._jvm.org.bigasterisk.api.BigAsterisk.perfdebug(
            spark._jsparkSession)

    def profile(self, df, top_k=20):
        """Measure the cost of each record leaving ``df``.

        Only the ``top_k`` most expensive records are retained; the rest are counted,
        so the mean and the skew ratio are exact.
        """
        if isinstance(df, str):
            df = self._spark.sql(df)
        return PerfProfile(self._support.profile(df._jdf, int(top_k)), self._spark)

    def active(self):
        """Every profile created and not yet cleared."""
        j = self._support.active()
        return [PerfProfile(j.apply(i), self._spark) for i in range(j.size())]

    def clear(self):
        """Forget every profile. Already-instrumented DataFrames keep working."""
        self._support.clear()
