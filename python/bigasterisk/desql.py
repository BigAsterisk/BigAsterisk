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


"""Step-through debugging for Spark SQL.

Decomposes a query into its constituent parts and gives you the intermediate data at
each one — the SQL equivalent of stepping through a program and watching a variable.

    import bigasterisk

    df = spark.sql("SELECT c.name, SUM(o.amount) AS total "
                   "FROM orders o JOIN customers c ON o.cid = c.cid "
                   "GROUP BY c.name")

    for step in bigasterisk.desql(spark).decompose(df):
        print(step)
        step.data.show()
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


class QueryStep:
    """One part of a decomposed query, with the rows it produces."""

    def __init__(self, jstep, spark):
        self._j = jstep
        self._spark = spark

    @property
    def id(self):
        """Position in the decomposition. Leaves first, the final result last."""
        return self._j.id()

    @property
    def operator(self):
        """The relational operator, e.g. ``Filter``, ``Join``, ``Aggregate``."""
        return self._j.operator()

    @property
    def detail(self):
        """The operator's expressions rendered as SQL text."""
        return self._j.detail()

    @property
    def child_ids(self):
        """Ids of the steps feeding this one, in operand order."""
        j = self._j.childIds()
        return [j.apply(i) for i in range(j.size())]

    @property
    def schema(self):
        """The schema of this step's intermediate result, as a PySpark ``StructType``.

        Read from the plan, so it costs nothing: asking what a step produces does not
        run it.
        """
        import json as _json
        from pyspark.sql.types import StructType
        return StructType.fromJson(_json.loads(self._j.schema().json()))

    @property
    def plan(self):
        """The whole sub-query this step computes, as a plan tree.

        ``detail`` is the step's *own* operator — a join condition, a grouping list.
        This is everything beneath it as well: the scans it reads, the filters already
        applied, the joins already made. Printing it answers "what have I got here?"
        without running anything.
        """
        return self._j.plan()

    @property
    def branches(self):
        """The conditional sub-operations of this step.

        A step that passes every record through — a projection, say — tells you nothing
        about which records a fault touched. Its branches do: of the records entering a
        ``CASE WHEN``, only some take the first arm. Branches inside a user-defined
        function appear here too, once the function has been read (see
        ``bigasterisk.udf``).

        Empty for steps with no conditional expressions, and for steps with more than
        one input.
        """
        j = self._j.branches()
        return [Branch(j.apply(i), self._spark) for i in range(j.size())]

    @property
    def data(self):
        """The intermediate rows at this step, as a PySpark DataFrame.

        Materialising this runs the query up to this point, and no further.

        :raises Py4JJavaError: if the step belongs to a correlated subquery, whose
            plan reads attributes from the enclosing query and cannot run alone.
        """
        return _wrap_dataframe(self._j.data(), self._spark)

    def __repr__(self):
        detail = (" — %s" % self.detail) if self.detail else ""
        return "[%d] %s%s" % (self.id, self.operator, detail)


class Branch:
    """One conditional sub-operation of a step, and the records that take it."""

    def __init__(self, jbranch, spark):
        self._j = jbranch
        self._spark = spark

    @property
    def description(self):
        """The condition, as SQL text."""
        return self._j.description()

    @property
    def data(self):
        """The step's input records that satisfy this branch, as a DataFrame."""
        return _wrap_dataframe(self._j.data(), self._spark)

    def __repr__(self):
        return "Branch(%s)" % self.description


class DeSql:
    """Query decomposition for a SparkSession."""

    def __init__(self, spark):
        self._spark = spark
        self._support = spark._jvm.org.bigasterisk.api.BigAsterisk.desql(
            spark._jsparkSession)

    def decompose(self, df):
        """Break ``df`` into its steps, ordered so children precede their consumer.

        Accepts either a DataFrame or a query string.
        """
        if isinstance(df, str):
            df = self._spark.sql(df)
        jsteps = self._support.decompose(df._jdf)
        return [QueryStep(jsteps.apply(i), self._spark) for i in range(jsteps.size())]
