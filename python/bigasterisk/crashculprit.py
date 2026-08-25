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

"""Crash-culprit determination: when a query dies on bad data, which record killed it.

A failing Spark job reports a stack trace and a task id. Neither says which of the
billion records being processed was the one the code could not handle, and bisecting the
input by hand is exactly the work this removes.

    import bigasterisk

    guard = bigasterisk.crash_culprit(spark).guard(orders)
    try:
        guard.df.selectExpr("parse_json(payload)").collect()
    except Exception:
        print(guard.culprit)
        # partition 0, record 7: {'oid': 'o8', 'cid': 'c2', 'amount': 99999}
        #   IllegalStateException: cannot handle 99999

This is BigDebug's crash-culprit primitive (ICSE 2016).
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


class CulpritRecord:
    """The record a query was processing when it failed."""

    def __init__(self, payload):
        self.partition_id = payload.pop("__partitionId")
        self.record_index = payload.pop("__recordIndex")
        self.error = payload.pop("__error")
        self.row = payload

    def __repr__(self):
        return "partition %d, record %d: %r\n  %s" % (
            self.partition_id, self.record_index, self.row, self.error)


class CrashCulprit:
    """A guard that remembers what a query was working on when it died."""

    def __init__(self, jguard, spark):
        self._j = jguard
        self._spark = spark

    @property
    def id(self):
        """Identifier, unique in the session."""
        return self._j.id()

    @property
    def df(self):
        """The instrumented DataFrame. Run this, or nothing is recorded."""
        return _wrap_dataframe(self._j.df(), self._spark)

    @property
    def culprit(self):
        """The record in flight when the query failed, or ``None`` if it has not.

        This does not work across a batched Python or Arrow UDF: the whole batch crosses
        to the worker before any of it can fail, so the record remembered is the last of
        the batch rather than the one that failed. Express the failing computation in SQL
        when you need the record named.
        """
        payload = self._j.culpritJson()
        return CulpritRecord(json.loads(payload)) if payload is not None else None

    def reset(self):
        """Forget what was recorded, so the guard can be reused for another run."""
        self._j.reset()

    def __repr__(self):
        culprit = self.culprit
        return "CrashCulprit(%s, %s)" % (
            self.id, "no failure" if culprit is None else repr(culprit))


class CrashCulpritGuards:
    """Crash-culprit determination for a SparkSession."""

    def __init__(self, spark):
        self._spark = spark
        self._support = spark._jvm.org.bigasterisk.api.BigAsterisk.crashCulprit(
            spark._jsparkSession)

    def guard(self, df):
        """Watch the records flowing out of ``df``, to name the one that kills the query.

        The cost is writing each record into a reused buffer — no allocation per record,
        and nothing moves to the driver unless something actually fails.
        """
        if isinstance(df, str):
            df = self._spark.sql(df)
        return CrashCulprit(self._support.guard(df._jdf), self._spark)

    def active(self):
        """Every guard created and not yet cleared."""
        j = self._support.active()
        return [CrashCulprit(j.apply(i), self._spark) for i in range(j.size())]

    def clear(self):
        """Forget every guard. Already-instrumented DataFrames keep working."""
        self._support.clear()
