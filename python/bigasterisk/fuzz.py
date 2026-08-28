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

"""Fuzz testing for Spark SQL: generate inputs for a query and see what breaks.

Testing a data-intensive application normally means running it over a real dataset,
which is slow and covers only the cases that dataset happens to contain. Fuzzing
generates small inputs instead, and steers them toward parts of the query nothing has
exercised yet.

Three mutation strategies, which are the three fuzzers this platform unifies:

``random``
    Values drawn at random for each column's type. Cheap, good at malformed values,
    poor at getting past a join. (BigFuzz)
``natural``
    Values spliced column-wise out of rows already seen, so generated rows look like
    real data while the combinations are new. (NaturalFuzz)
``co-dependent``
    Splicing that draws joined columns from a shared pool, so generated rows survive
    the join. The default. (DepFuzz)

    import bigasterisk

    result = bigasterisk.fuzz(spark).fuzz(
        "SELECT c.name, SUM(o.amount) FROM orders o JOIN customers c ON o.cid = c.cid "
        "GROUP BY c.name",
        {"orders": orders, "customers": customers},
        iterations=100)

    print(result.coverage)
    for failure in result.failures:
        print(failure)
"""

import json

from .query import as_query


class FuzzFailure:
    """An input that made the query fail."""

    def __init__(self, payload):
        self.iteration = payload["iteration"]
        self.error = payload["error"]
        self.tables = payload["tables"]

    def __repr__(self):
        rows = "\n".join(
            "  %s: %s" % (name, ", ".join(rows)) for name, rows in self.tables.items())
        return "iteration %d: %s\n%s" % (self.iteration, self.error, rows)


class FuzzResult:
    """What a fuzzing campaign found."""

    def __init__(self, payload):
        self.iterations = payload["iterations"]
        self.total_branches = payload["totalBranches"]
        self.empty_results = payload["emptyResults"]
        self.coverage = payload["coverage"]
        self.abstracted = payload["abstracted"]
        self.covered = set(payload["covered"])
        self.failures = [FuzzFailure(f) for f in payload["failures"]]
        #: a few of the generated inputs, so you can see what the campaign fed the query
        self.samples = [FuzzSample(s) for s in payload.get("samples", [])]

    def __repr__(self):
        return ("FuzzResult(%d iterations (%d without Spark), %d failures, "
                "coverage %.0f%% of %d branches, %d empty)" % (
                    self.iterations, self.abstracted, len(self.failures),
                    self.coverage * 100, self.total_branches, self.empty_results))


class FuzzSample:
    """One input the campaign generated, kept so it can be looked at."""

    def __init__(self, payload):
        self.iteration = payload["iteration"]
        #: whether it reached a branch nothing had reached before
        self.reached_new = payload["reachedNew"]
        #: whether the query returned nothing for it
        self.empty = payload["empty"]
        #: the generated rows, by table name, each row rendered as text
        self.tables = payload["tables"]

    def __repr__(self):
        note = []
        if self.reached_new:
            note.append("reached new coverage")
        if self.empty:
            note.append("produced no output")
        suffix = "  (%s)" % ", ".join(note) if note else ""
        body = "\n".join(
            "  %s: %s%s" % (name, ", ".join(rows[:4]),
                            ", ... %d rows" % len(rows) if len(rows) > 4 else "")
            for name, rows in sorted(self.tables.items()))
        return "iteration %d%s\n%s" % (self.iteration, suffix, body)


class Fuzz:
    """Fuzz testing for a SparkSession."""

    def __init__(self, spark):
        self._spark = spark
        self._support = spark._jvm.org.bigasterisk.api.BigAsterisk.fuzz(
            spark._jsparkSession)

    def fuzz(self, query, seeds, iterations=100, strategy="co-dependent",
             rows_per_table=10, seed=0, guided=True, abstract_framework=True,
             rows_per_vector=3, keep_samples=3):
        """Run a fuzzing campaign against ``query``.

        ``query`` is a DataFrame — the pipeline itself — or a SQL string. Either way
        the campaign has to run it repeatedly with data of its own choosing; for a
        DataFrame that is done by substituting into its plan, so each seed must be the
        DataFrame the pipeline was built from (or a table it reads under that name).

        ``seeds`` maps each table name the query reads to a DataFrame. Their rows are
        the corpus generated values are drawn from; only the schema matters for the
        ``random`` strategy.

        With ``abstract_framework`` (the default), iterations are evaluated by
        interpreting the query's plan over in-memory rows rather than running a Spark
        job — the operator semantics are Spark's either way, but the planning,
        scheduling and task setup that dwarf a twenty-row query are gone. Anything the
        interpreter does not support falls back to Spark, so this changes speed and
        never results.

        ``keep_samples`` retains that many of the generated inputs on the result, so you
        can look at what the campaign actually fed the query — see ``FuzzResult.samples``.
        Inputs that reached new coverage are preferred over merely early ones.

        ``rows_per_vector`` bounds the corpus: seed rows that decide every branch of the
        query the same way are interchangeable to its control flow, so only a few of each
        distinct behaviour are kept.

        The campaign swaps generated data in under those table names while it runs and
        restores the originals afterwards.
        """
        jseeds = self._spark._jvm.java.util.HashMap()
        for name, df in seeds.items():
            jseeds.put(name, df._jdf)
        result = self._support.fuzzJava(
            as_query(query), jseeds, int(iterations), strategy, int(rows_per_table),
            int(seed), bool(guided), bool(abstract_framework), int(rows_per_vector),
            int(keep_samples))
        return FuzzResult(json.loads(result.json()))
