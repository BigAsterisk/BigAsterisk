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


"""Fault-inducing operation isolation.

Data provenance answers "which records produced this?". It says nothing about *which
part of the query* was at fault. OptDebug closes that gap by scoring a query's
operations the way spectrum-based fault localisation scores lines of code: an operation
that most failing records passed through, and few passing records did, is the one to
look at.

    import bigasterisk

    result = bigasterisk.optdebug(spark).localize(
        "SELECT cid, SUM(CASE WHEN amount > 1000 THEN -amount ELSE amount END) AS total "
        "FROM orders GROUP BY cid",
        faulty_where="total < 0")          # a negative total is wrong

    for op in result.ranked:
        print(op)
    # 1.000  [1] Aggregate branch — (amount > 1000)  (failing=1, passing=0)

From *OptDebug: Fault-Inducing Operation Isolation for Dataflow Applications*
(SoCC 2021).
"""

from .query import as_query


class SuspiciousOperation:
    """One operation of a query, scored by how responsible it looks."""

    def __init__(self, joperation):
        self._j = joperation

    @property
    def step_id(self):
        """The operation's position in the query's decomposition."""
        return self._j.stepId()

    @property
    def operator(self):
        """The relational operator, e.g. ``Filter``, ``Join``, ``Aggregate``."""
        return self._j.operator()

    @property
    def detail(self):
        """The operator's expressions, as SQL text."""
        return self._j.detail()

    @property
    def branch(self):
        """The conditional arm scored, or ``None`` when this scores the whole operator."""
        return self._j.branchOrNull()

    @property
    def is_branch(self):
        """True when this scores one arm of a conditional rather than the operator."""
        return self._j.isBranch()

    @property
    def failing_witnesses(self):
        """Failing source records that reached this operation."""
        return self._j.failingWitnesses()

    @property
    def passing_witnesses(self):
        """Passing source records that reached this operation."""
        return self._j.passingWitnesses()

    @property
    def score(self):
        """Suspiciousness in ``[0, 1]``; higher is more suspicious."""
        return self._j.score()

    def __repr__(self):
        return self._j.toString()


class OptDebugResult:
    """The outcome of localising a fault to the operations of a query."""

    def __init__(self, jresult):
        self._j = jresult

    @property
    def ranked(self):
        """Operations, most suspicious first."""
        j = self._j.ranked()
        return [SuspiciousOperation(j.apply(i)) for i in range(j.size())]

    @property
    def prime(self):
        """The most suspicious operation, or ``None`` if there was nothing to score."""
        ops = self.ranked
        return ops[0] if ops else None

    @property
    def failing_witnesses(self):
        """How many source records lie behind the rejected outputs."""
        return self._j.failingWitnesses()

    @property
    def passing_witnesses(self):
        """How many source records lie behind the accepted outputs."""
        return self._j.passingWitnesses()

    @property
    def formula(self):
        """The scoring formula used."""
        return self._j.formula()

    @property
    def minimised(self):
        """Whether the failing population was narrowed before scoring."""
        return self._j.minimised()

    @property
    def minimised_from(self):
        """How many failing witnesses there were before narrowing, or ``None``."""
        opt = self._j.minimisedFrom()
        return opt.get() if opt.isDefined() else None

    def __repr__(self):
        narrowing = "" if not self.minimised else " (minimised from %d)" % self.minimised_from
        return "OptDebugResult(%s, %d ranked, failing=%d%s, passing=%d)" % (
            self.formula, len(self.ranked), self.failing_witnesses, narrowing,
            self.passing_witnesses)


class OptDebug:
    """Fault-inducing operation isolation for a SparkSession."""

    def __init__(self, spark):
        self._spark = spark
        self._jvm = spark._jvm

    def localize(self, df, faulty_where, formula="tarantula", base_table=None):
        """Rank the operations of ``df`` by how responsible each looks.

        ``df`` is a DataFrame or a query string. ``faulty_where`` is a SQL predicate
        over the query's output columns that is true for a *wrong* row — a predicate
        rather than a callback, because it has to be evaluated inside the JVM.

        ``formula`` is ``"tarantula"`` (the default) or ``"ochiai"``.

        Pass ``base_table`` to narrow the failing records to those that actually cause
        the failure before scoring. Narrowing re-runs the query with the table
        restricted, once per subset tested, and makes ``"ochiai"`` viable, which it is
        not without it. A DataFrame is re-run by substituting into its plan, so
        ``base_table`` must name a table the pipeline actually reads.
        """
        opt = self._jvm.org.bigasterisk.optdebug.OptDebug

        if base_table is not None:
            return OptDebugResult(opt.localize(
                self._spark._jsparkSession, base_table, as_query(df), faulty_where,
                opt.formulaByName(formula)))

        if isinstance(df, str):
            df = self._spark.sql(df)
        return OptDebugResult(opt.localize(
            self._spark._jsparkSession, df._jdf, faulty_where,
            opt.formulaByName(formula)))
