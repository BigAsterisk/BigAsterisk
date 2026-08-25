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


"""Record-level data provenance over PySpark DataFrames.

Thin wrapper over ``org.bigasterisk.api.LineageSupport``: the binding for the running
Spark version is resolved on the JVM side, so this module never names an engine class.
"""

import json


class TraceCursor:
    """A position in the backward/forward provenance walk.

    Immutable: :meth:`go_back` and :meth:`go_next` return new cursors, so a walk can
    be branched and revisited.
    """

    def __init__(self, jcursor):
        self._j = jcursor

    @property
    def ids(self):
        """Packed ``(partition, rowIdx)`` lineage ids at the current level."""
        return list(self._j.ids())

    @property
    def at_scan(self):
        """True when the cursor has reached a source scan — ``go_back`` stops here."""
        return self._j.atScan()

    def go_back(self, branch=0):
        """One step backward. ``branch`` selects the input at a join (0 = build side)."""
        return TraceCursor(self._j.goBack(branch))

    def go_next(self):
        """One step forward, retracing the last :meth:`go_back`."""
        return TraceCursor(self._j.goNext())

    def show(self, full=False):
        """Records at this position, as a list of dicts. Only meaningful at a scan.

        With ``full=True`` returns complete source records rather than the query's
        column-pruned view, when the scan shape allows it.
        """
        return [json.loads(s) for s in self._j.showJson(full)]

    def to_scan(self, max_hops=32):
        """Walk backward until the source scan is reached, and return that cursor.

        Follows branch 0 at every join. ``max_hops`` bounds the walk so a malformed
        capture graph cannot loop forever.

        :raises RuntimeError: if the scan is not reached within ``max_hops``.
        """
        cursor, hops = self, 0
        while not cursor.at_scan:
            if hops >= max_hops:
                raise RuntimeError(
                    "did not reach a source scan within %d hops; the capture graph may "
                    "be deeper than expected — raise max_hops or walk manually"
                    % max_hops)
            cursor = cursor.go_back(0)
            hops += 1
        return cursor


class Lineage:
    """Driver-side capture and trace for PySpark DataFrames."""

    def __init__(self, spark):
        self._spark = spark
        jvm = spark._jvm
        # Resolving through the platform entry point performs binding selection and
        # validates that the session was built with the SQL extension registered.
        self._support = jvm.org.bigasterisk.api.BigAsterisk.lineage(spark._jsparkSession)
        self._jvm = jvm

    def enable_capture(self):
        """Turn capture on for subsequent queries in this session."""
        self._support.enableCapture(self._spark._jsparkSession)

    def disable_capture(self):
        """Turn capture off. Already-captured lineage stays available until released."""
        self._support.disableCapture(self._spark._jsparkSession)

    def collect_with_lineage(self, df):
        """Collect ``df`` and return ``[(Row, lineage_id), ...]``."""
        rows = df.collect()
        ids = list(self._support.resultIds(df._jdf))
        if len(rows) != len(ids):
            raise RuntimeError(
                "capture mismatch: %d rows but %d lineage ids. This usually means "
                "capture was not enabled when the query ran." % (len(rows), len(ids)))
        return list(zip(rows, ids))

    def trace(self, df, output_ids):
        """Open a cursor standing on the given output ids of ``df``."""
        jlist = self._jvm.java.util.ArrayList()
        for i in output_ids:
            jlist.add(self._jvm.java.lang.Long(int(i)))
        return TraceCursor(self._support.traceJava(df._jdf, jlist))

    def release_lineage(self, df):
        """Drop the lineage blocks held for ``df``. Safe to call more than once."""
        self._support.releaseLineage(df._jdf)

    def lineage_size(self, df):
        """The retained lineage footprint for ``df`` as ``(records, bytes)``."""
        t = self._support.lineageSize(df._jdf)
        return (t._1(), t._2())
