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

"""Simulated breakpoints: inspect a query's state at a point, without pausing anything.

Stepping through a distributed job the way a conventional debugger steps through a
program is not affordable — halting every executor to look at one intermediate value
throws away the throughput the job exists for. A simulated breakpoint gives the same
experience without the halt: it records what is needed to regenerate the state at that
point, and regenerates it when, and only when, someone looks.

Setting one costs nothing. No operator is inserted, nothing is captured while the query
runs, and a breakpoint that is never inspected is free.

    import bigasterisk

    bp = bigasterisk.breakpoints(spark).breakpoint(orders.filter("amount > 100"))

    # the rest of the query is built on the breakpoint and runs at full speed
    bp.df.groupBy("cid").sum("amount").collect()

    # afterwards, look at what was flowing past that point
    for row in bp.state():
        print(row)
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


class Breakpoint:
    """A point in a query whose state can be inspected on demand."""

    def __init__(self, jbreakpoint, spark):
        self._j = jbreakpoint
        self._spark = spark

    @property
    def id(self):
        """Identifier, unique in the session."""
        return self._j.id()

    @property
    def df(self):
        """The query as it stands at this point. Build the rest of the query on this."""
        return _wrap_dataframe(self._j.df(), self._spark)

    @property
    def schema(self):
        """The shape of the state here, available without computing it."""
        return self.df.schema

    def state(self, limit=20):
        """The records flowing past this point, regenerated on demand.

        Served from the pinned state when :meth:`materialize` has been called; otherwise
        the query prefix up to this point is re-executed.
        """
        if limit < 0:
            raise ValueError("limit must not be negative, got %d" % limit)
        return self.df.limit(limit).collect()

    def count(self):
        """How many records pass this point."""
        return self._j.count()

    def materialize(self):
        """Pin the state here, so repeated inspection starts from it.

        This is the trade the technique is built on: a breakpoint costs nothing until
        you decide you are going to look more than once.
        """
        self._j.materialize()

    @property
    def is_materialized(self):
        """True when the state here has been pinned."""
        return self._j.isMaterialized()

    def release(self):
        """Unpin the state."""
        self._j.release()

    def resume_with(self, continue_fn):
        """Resume the computation from this point, through ``continue_fn``.

        ``continue_fn`` need not be what the original query did. Correcting the step
        after a breakpoint and re-running from there — rather than from the beginning —
        is the on-the-fly fix this is for::

            bp.resume_with(lambda d: d.filter("amount <= 1000").groupBy("cid").sum("amount"))
        """
        return continue_fn(self.df)

    def __repr__(self):
        pinned = ", materialized" if self.is_materialized else ""
        return "Breakpoint(%s, %s%s)" % (
            self.id, ", ".join(self.schema.fieldNames()), pinned)


class Breakpoints:
    """Simulated breakpoints for a SparkSession."""

    def __init__(self, spark):
        self._spark = spark
        self._support = spark._jvm.org.bigasterisk.api.BigAsterisk.breakpoints(
            spark._jsparkSession)

    def breakpoint(self, df):
        """Set a breakpoint at ``df``. Costs nothing until the state is inspected."""
        if isinstance(df, str):
            df = self._spark.sql(df)
        return Breakpoint(self._support.breakpoint(df._jdf), self._spark)

    def active(self):
        """Every breakpoint set and not yet cleared."""
        j = self._support.active()
        return [Breakpoint(j.apply(i), self._spark) for i in range(j.size())]

    def clear(self):
        """Remove every breakpoint, releasing anything they pinned."""
        self._support.clear()
