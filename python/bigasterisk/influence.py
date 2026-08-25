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


"""Influence-based provenance: of the records behind a result, which ones mattered.

Ordinary provenance answers a yes/no question — did this record contribute? For a
many-to-one dependency that answer is nearly useless: every record of a group
contributed to its aggregate, so tracing a wrong MAX over a million-row group returns a
million records.

Influence-based provenance asks *how much* each contributed, by reading the aggregate's
semantics. Only the largest record influences a MAX; a record's influence on a SUM is
the size of its contribution.

    import bigasterisk

    ranked = bigasterisk.influence(spark).influencers(
        "SELECT cid, MAX(amount) AS peak FROM orders GROUP BY cid",
        faulty_where="peak > 1000")

    print(ranked[0])
    # 1.0000  {'oid': 'o8', 'cid': 'c2', 'amount': 99999}  (only the maximum influences)

From *Influence-Based Provenance for Dataflow Applications with Taint Propagation*
(SoCC 2020).
"""

import json


class Influence:
    """One input record, and how much it influenced a result."""

    def __init__(self, payload):
        self.score = payload.pop("__score")
        self.reason = payload.pop("__reason")
        self.row = payload

    def __repr__(self):
        return "%.4f  %r  (%s)" % (self.score, self.row, self.reason)


class InfluenceProvenance:
    """Influence-based provenance for a SparkSession."""

    def __init__(self, spark):
        self._spark = spark
        self._support = spark._jvm.org.bigasterisk.api.BigAsterisk.influence(
            spark._jsparkSession)

    def influencers(self, df, faulty_where, top_k=20):
        """Rank the records behind the results ``faulty_where`` selects.

        ``df`` is a DataFrame or a query string. ``faulty_where`` is a SQL predicate
        over the aggregation's output naming the results to explain.

        Returns :class:`Influence` entries, most influential first.
        """
        if isinstance(df, str):
            df = self._spark.sql(df)
        return [Influence(json.loads(s))
                for s in self._support.influencersJson(df._jdf, faulty_where, int(top_k))]
