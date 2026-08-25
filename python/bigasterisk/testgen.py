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

"""Systematic test-input generation for Spark SQL.

Fuzzing searches for inputs by mutating what it has. This works the other way round: it
reads the query's own conditions, solves them, and constructs one input per path through
them. Where fuzzing eventually stumbles onto a branch, this builds a record that takes
it.

This is BigTest's approach; with ``natural=True`` (the default) it is NaturalSym's — the
same paths, but with witnesses drawn from values that really occur, so the generated
tests read like data rather than like solver output.

    import bigasterisk

    suite = bigasterisk.testgen(spark).generate(
        "SELECT cid FROM orders WHERE amount > 100", {"orders": orders})

    print(suite.coverage)
    for case in suite.cases:
        print(case)

Every generated input is executed and the branch it was built for is checked, so
``case.verified`` says what actually happened rather than what was intended.
"""

import json


class TestCase:
    """An input built to drive the query down a particular path."""

    def __init__(self, payload):
        self.id = payload["id"]
        self.path = payload["path"]
        self.verified = payload["verified"]
        self.note = payload["note"]
        self.tables = payload["tables"]

    def __repr__(self):
        mark = "ok" if self.verified else "--"
        rows = "\n".join(
            "    %s: %s" % (name, ", ".join(rows)) for name, rows in self.tables.items())
        return "[%s] %s  (%s)\n%s" % (mark, self.path, self.note, rows)


class TestSuite:
    """What a test-generation run produced."""

    def __init__(self, payload):
        self.total_branches = payload["totalBranches"]
        self.coverage = payload["coverage"]
        self.cases = [TestCase(c) for c in payload["cases"]]

    @property
    def verified(self):
        """Tests whose path was reached when the input was actually run."""
        return [c for c in self.cases if c.verified]

    def __repr__(self):
        return "TestSuite(%d cases, %d verified, %d branches)" % (
            len(self.cases), len(self.verified), self.total_branches)


class TestGen:
    """Systematic test-input generation for a SparkSession."""

    def __init__(self, spark):
        self._spark = spark
        self._support = spark._jvm.org.bigasterisk.api.BigAsterisk.testgen(
            spark._jsparkSession)

    def generate(self, query, seeds, max_paths=32, rows_per_path=3, natural=True,
                 seed=0, distributions=None):
        """Generate a test suite for ``query``.

        ``seeds`` maps each table name the query reads to a DataFrame. Schemas are
        required; the rows are the pool of natural witnesses.

        ``distributions`` declares the shape of individual columns by name, as text::

            distributions={"score": "binom(100, 0.1)",
                           "name": 'Discrete("alice", "bob")'}

        You know the shape of your own data; the solver does not, and left to itself
        satisfies ``age > 18`` with ``19`` every time. Where a column is declared,
        witnesses are drawn from that distribution and kept if they satisfy the path —
        so naturalness never costs coverage.

        Generation swaps its inputs in under those table names while it runs and
        restores the originals afterwards.
        """
        jseeds = self._spark._jvm.java.util.HashMap()
        for name, df in seeds.items():
            jseeds.put(name, df._jdf)
        jdists = self._spark._jvm.java.util.HashMap()
        for column, spec in (distributions or {}).items():
            jdists.put(column, spec)
        suite = self._support.generateJava(
            query, jseeds, int(max_paths), int(rows_per_path), bool(natural), int(seed),
            jdists)
        return TestSuite(json.loads(suite.json()))
