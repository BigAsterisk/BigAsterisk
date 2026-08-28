# A realistic PySpark pipeline, one planted fault, and every tool on it.
#
#   scripts/cluster.sh run airline           # against the Docker cluster
#   python/demos/run-airline.sh              # against a local Spark
#
# The notebook `notebooks/airline_analysis.ipynb` is this, with the narrative.

import os
import sys
import tempfile
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, lit, substring, udf
from pyspark.sql.types import IntegerType, StringType

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import bigasterisk                                            # noqa: E402
from demos import airline_data                                # noqa: E402

FLIGHTS = int(os.environ.get("FLIGHTS", "250000"))
SEED = int(os.environ.get("SEED", "7"))


# ---------------------------------------------------------------------------
# Progress reporting: every tool says what it is about to do, what it found, and
# how long it took. A tool that prints only its answer is impossible to trust.
# ---------------------------------------------------------------------------

class Step(object):
    number = 0

    def __init__(self, tool, question, how):
        Step.number += 1
        self.tool, self.question, self.how = tool, question, how

    def __enter__(self):
        print("\n" + "=" * 78)
        print("  %d. %s" % (Step.number, self.tool))
        print("     question: %s" % self.question)
        print("     method:   %s" % self.how)
        print("=" * 78)
        sys.stdout.flush()
        self.started = time.time()
        return self

    def say(self, line=""):
        print("     %s" % line)
        sys.stdout.flush()

    def finding(self, line):
        print("  >> %s" % line)
        sys.stdout.flush()

    def __exit__(self, kind, value, traceback):
        elapsed = time.time() - self.started
        if kind is None:
            print("     (%.1fs)" % elapsed)
        else:
            print("     FAILED after %.1fs: %s: %s" % (elapsed, kind.__name__, value))
        sys.stdout.flush()
        return False


def main():
    spark = bigasterisk.configure(
        SparkSession.builder.appName("airline analysis")
        .config("spark.sql.shuffle.partitions", "8")).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("connected to %s" % spark.sparkContext.master)
    run(spark)
    spark.stop()


def run(spark):
    # -----------------------------------------------------------------------
    # The data
    # -----------------------------------------------------------------------
    started = time.time()
    flights = spark.createDataFrame(
        spark.sparkContext.parallelize(airline_data.flights(FLIGHTS, seed=SEED), 8),
        airline_data.FLIGHTS_SCHEMA)
    airports = spark.createDataFrame(airline_data.airports(), airline_data.AIRPORTS_SCHEMA)
    carriers = spark.createDataFrame(airline_data.carriers(), airline_data.CARRIERS_SCHEMA)

    # Written out and read back: a file scan is what provenance capture attaches to, and
    # it is also how the real feed arrives.
    root = os.environ.get(
        "AIRLINE_DATA",
        os.path.join(tempfile.gettempdir(), "bigasterisk-airline"))
    for name, df in (("flights", flights), ("airports", airports), ("carriers", carriers)):
        path = os.path.join(root, name)
        if not os.path.isdir(path):
            df.write.mode("overwrite").parquet(path)

    flights = spark.read.parquet(os.path.join(root, "flights"))
    airports = spark.read.parquet(os.path.join(root, "airports"))
    carriers = spark.read.parquet(os.path.join(root, "carriers"))
    for name, df in (("flights", flights), ("airports", airports), ("carriers", carriers)):
        df.createOrReplaceTempView(name)

    print("\n%d flights, %d airports, %d carriers  (%.1fs)"
          % (flights.count(), airports.count(), carriers.count(), time.time() - started))

    # -----------------------------------------------------------------------
    # The pipeline: two Python UDFs, two joins, one aggregation
    #
    # Written with the DataFrame API and ordinary Python functions, because that is
    # what a PySpark job looks like. Nothing here is arranged for the tools' benefit:
    # they are given this pipeline and have to work with it as it is.
    # -----------------------------------------------------------------------

    @udf(returnType=StringType())
    def haul(distance):
        """Route length class."""
        if distance < 500:
            return "short"
        elif distance < 1500:
            return "medium"
        return "long"

    @udf(returnType=IntegerType())
    def adjusted_delay(arr_delay):
        """Arrival delay, with extreme values 'corrected'.

        The fault: the correction was meant to clamp implausible delays and instead
        flips their sign. It only fires above 300 minutes, so most flights are fine and
        the aggregate looks nearly right.
        """
        if arr_delay is None:
            return None
        if arr_delay > 300:
            return -arr_delay
        return arr_delay

    def analysis(flights, carriers, airports):
        """Average delay per carrier and route length.

        Taking the three inputs as arguments is not a concession to the tools; it is
        how a pipeline stays testable. It also happens to be exactly what several of
        these tools want — a watchpoint or a profiler hands back a wrapped DataFrame,
        and running the analysis over it is then a function call rather than a rewrite.
        """
        # `code` and `name` are in both reference tables, so each is narrowed to what
        # this analysis needs before the join rather than disambiguated at every use.
        named = carriers.select(col("code").alias("carrier_code"),
                                col("name").alias("carrier"))
        origins = airports.select(col("code").alias("origin_code"))

        flown = flights.filter(col("cancelled") == 0)
        return (flown
                .join(named, flown["carrier"] == named["carrier_code"])
                .join(origins, flown["origin"] == origins["origin_code"])
                .withColumn("haul", haul(flown["distance"]))
                .groupBy(named["carrier"], col("haul"))
                .agg(count("*").alias("flights"),
                     avg(adjusted_delay(flown["arr_delay"])).alias("avg_delay")))

    # `spark.table(...)` rather than the DataFrames above: the tools that re-run this
    # pipeline with substitute data find `flights` in its plan by that name.
    def pipeline():
        return analysis(spark.table("flights"), spark.table("carriers"),
                        spark.table("airports"))

    with Step("The pipeline", "what does the analysis say?",
              "two joins, two Python UDFs, one grouped aggregate") as step:
        result = pipeline().orderBy("avg_delay")
        rows = result.collect()
        step.say("%d (carrier, haul) groups" % len(rows))
        step.say("")
        step.say("%-24s %-8s %8s %10s" % ("carrier", "haul", "flights", "avg_delay"))
        for row in rows[:5]:
            step.say("%-24s %-8s %8d %10.1f"
                     % (row["carrier"], row["haul"], row["flights"], row["avg_delay"]))
        step.finding("the worst groups average large NEGATIVE delays — "
                     "no airline lands an hour early on average")

    ORACLE = "avg_delay < -20"
    wrong = pipeline().filter(ORACLE).collect()
    print("\n%d of %d groups are implausible (%s)" % (len(wrong), len(rows), ORACLE))

    # ---------------------------------------------------------------------------
    # 2. DeSQL — what is this query actually made of?
    # ---------------------------------------------------------------------------
    with Step("DeSQL", "what are the parts of this query?",
              "decompose the plan; each step can be materialised on its own") as step:
        steps = bigasterisk.desql(spark).decompose(pipeline())
        for s in steps:
            step.say("[%d] %-12s %s" % (s.id, s.operator, (s.detail or "")[:58]))
        step.finding("%d steps, %d of them carrying conditional branches"
                     % (len(steps), sum(1 for s in steps if s.branches)))

    # ---------------------------------------------------------------------------
    # 3. Titian — which records produced a wrong group?
    # ---------------------------------------------------------------------------
    # the worst group, deterministically: collect order is not stable, and every step
    # after this one depends on which group we chase
    worst = sorted(wrong, key=lambda r: r["avg_delay"])[0] if wrong else rows[0]
    target = "carrier = '%s' AND haul = '%s'" % (worst["carrier"], worst["haul"])

    with Step("Titian", "which input records produced %s?" % target,
              "record-level provenance captured during execution, traced to the scan") as step:
        lineage = bigasterisk.lineage(spark)
        step.say("enabling capture and re-running the query...")
        lineage.enable_capture()
        try:
            traced = pipeline()
            outputs = lineage.collect_with_lineage(traced)
            step.say("captured lineage for %d output groups" % len(outputs))
            picked = [(row, ids) for row, ids in outputs
                      if row["carrier"] == worst["carrier"] and row["haul"] == worst["haul"]]
            cursor = lineage.trace(traced, [picked[0][1]])
            hops = 0
            while not cursor.at_scan:
                cursor = cursor.go_back()
                hops += 1
            witnesses = cursor.show()
            step.say("walked back %d step(s) to the source scan" % hops)
            step.finding("%d source flights produced that one group" % len(witnesses))
            step.say("provenance is exact — and %d records is still too many to read"
                     % len(witnesses))
        finally:
            lineage.disable_capture()

    # ---------------------------------------------------------------------------
    # 4. FlowDebug — which of those records actually mattered?
    # ---------------------------------------------------------------------------
    with Step("FlowDebug", "of those flights, which one is responsible?",
              "influence from the aggregate's semantics, plus taint through the UDF") as step:
        influence_ranked = bigasterisk.influence(spark).influencers(
            pipeline(), target, top_k=5)
        ranked = influence_ranked
        for influence in ranked[:4]:
            step.say("%.4f  %s" % (influence.score, _flight(influence.row)))
        if ranked:
            step.finding("the top-ranked flight carries %.1f%% of the responsibility, "
                         "out of %d witnesses"
                         % (ranked[0].score * 100, len(witnesses)))
            if ranked[0].columns:
                step.say("columns that could reach the result: %s"
                         % ", ".join(sorted(ranked[0].columns)))

    # ---------------------------------------------------------------------------
    # 5. Reading the UDFs — the boundary the next tools need crossed
    # ---------------------------------------------------------------------------
    with Step("UDF analysis", "what is inside haul() and adjusted_delay()?",
              "parse the Python source; branches, paths, and which arguments matter") as step:
        for function in (haul, adjusted_delay):
            profile = bigasterisk.udf.register(spark, function)
            step.say("%s" % profile)
            for condition, params in profile.branches:
                step.say("    branch: %s" % condition)
            for constraint, returns, exact in profile.paths:
                # a path that returns a computed value is still a coverage target; it
                # just cannot be inverted, so no literal is recorded for it
                step.say("    path:   %-46s -> %s"
                         % (constraint, returns if returns else "(computed)"))
        step.finding("the UDFs are no longer black boxes: their branches are now "
                     "conditions over the columns the query passes them")

    # ---------------------------------------------------------------------------
    # 6. OptDebug — which OPERATION is wrong, not which record
    # ---------------------------------------------------------------------------
    # These tools re-execute the query many times, so they work on a sample. The sample is
    # built deliberately: a clean slice of the feed, plus the single flight influence
    # named. Anything else gives spectrum-based ranking almost nothing to discriminate
    # against — put a malformed record in every group and every group is faulty, so every
    # operation ties at the top and the ranking says nothing.
    culprit_id = influence_ranked[0].row["flight_id"]
    scoped_path = os.path.join(root, "flights_scoped")
    clean = spark.table("flights").filter(
        # every twentieth flight, chosen by its id rather than by sampling: `sample()` depends
        # on how the file splits happen to be assigned, so it is not reproducible across runs
        (col("arr_delay").isNull() | (col("arr_delay") < 10000))
        & (substring(col("flight_id"), 2, 12).cast("int") % 20 == 0))
    scoped = clean.unionByName(
        spark.table("flights").filter(col("flight_id") == culprit_id))
    scoped.write.mode("overwrite").parquet(scoped_path)
    spark.read.parquet(scoped_path).createOrReplaceTempView("flights")
    scoped_count = spark.table("flights").count()

    with Step("OptDebug", "which operation produces the wrong number?",
              "score every operation and branch by the records that reach it") as step:
        step.say("a clean %d-flight sample, plus the one flight influence named (%s)"
                 % (scoped_count, culprit_id))
        # base_table narrows the failing records to those that actually cause the
        # failure *before* scoring. Without it the branch `arr_delay > 300` is taken by
        # thousands of legitimately late flights as well as by the malformed one, and a
        # spectrum cannot separate them.
        result = bigasterisk.optdebug(spark).localize(
            pipeline(), ORACLE, base_table="flights")
        optdebug_result = result
        if result.minimised:
            step.say("narrowed the failing input from %d records before scoring"
                     % result.minimised_from)
        for operation in result.ranked[:8]:
            step.say("%.3f  %s" % (operation.score, _operation(operation)))
        inside = [(i, op) for i, op in enumerate(result.ranked, 1)
                  if op.branch and ("arr_delay" in op.branch or "distance" in op.branch)]
        if inside:
            rank, op = inside[0]
            step.finding("highest-ranked branch from inside a UDF is #%d: %s (%.3f)"
                         % (rank, op.branch, op.score))
        step.say("those branches are invisible to plan analysis; they are here only "
                 "because the functions were read")

    # ---------------------------------------------------------------------------
    # 7. BigSift — the smallest input that still reproduces it
    # ---------------------------------------------------------------------------
    with Step("BigSift", "what is the smallest input that still fails?",
              "delta debugging over the provenance, re-running to check each subset") as step:
        step.say("starting from %d flights..." % scoped_count)
        outcome = bigasterisk.BigSift(spark).debug(
            "flights", pipeline(),
            lambda row: row["avg_delay"] is not None and row["avg_delay"] < -20)
        step.say("provenance left %d candidate records" % outcome.provenance_size)
        for row in outcome.fault_inducing_rows[:3]:
            step.say("    %s" % _flight(row))
        step.finding("%d record(s) reproduce the failure on their own"
                     % len(outcome.fault_inducing_rows))

    spark.read.parquet(os.path.join(root, "flights")).createOrReplaceTempView("flights")

    # ---------------------------------------------------------------------------
    # 8. BigDebug — the interactive primitives
    # ---------------------------------------------------------------------------
    with Step("BigDebug", "can I watch the feed and survive a crash?",
              "a watchpoint on the records flowing past, and a guard that names the "
              "record that kills a task") as step:
        # The whole change to the application: read `flights` through the watchpoint
        # instead of directly. Because the pipeline takes its inputs as arguments, that
        # is one substitution at the call — nothing inside the analysis moves.
        watched = bigasterisk.watchpoints(spark).watch(
            spark.table("flights"), col("arr_delay") > 10000)
        analysis(watched.df, spark.table("carriers"), spark.table("airports")).collect()
        step.say("watchpoint `%s` matched %d record(s)" % (watched.condition, watched.hits))
        for row in watched.captured[:3]:
            step.say("    %s" % _flight(row))

        # a query that dies on the malformed record, and a guard that catches it
        step.say("")
        step.say("now a query that divides by the distance from that outlier...")
        guard = bigasterisk.crash_culprit(spark).guard(
            spark.table("flights").filter(col("arr_delay").isNotNull()).coalesce(1))
        previous = spark.conf.get("spark.sql.ansi.enabled")
        spark.conf.set("spark.sql.ansi.enabled", "true")
        # The failure is expected, and Spark logs a full query context for it through a
        # logger of its own that setLogLevel does not reach. Silence that one by name so
        # the demo shows the culprit rather than a page of stack trace.
        _quieten(spark, ["SQLQueryContextLogger", "org.apache.spark.scheduler.TaskSetManager"])
        spark.sparkContext.setLogLevel("OFF")
        try:
            guard.df.select(
                col("flight_id"),
                (lit(100) / (col("arr_delay") - 100000)).alias("boom")).collect()
            step.say("expected a failure and did not get one")
        except Exception:
            culprit = guard.culprit
            if culprit:
                step.finding("the record that killed the task: %s" % _flight(culprit.row))
                step.say("partition %d, record %d" % (culprit.partition_id, culprit.record_index))
        finally:
            spark.sparkContext.setLogLevel("ERROR")
            _quieten(spark, ["SQLQueryContextLogger",
                             "org.apache.spark.scheduler.TaskSetManager"], "ERROR")
            spark.conf.set("spark.sql.ansi.enabled", previous)

    # ---------------------------------------------------------------------------
    # 9. PerfDebug — where does the time go?
    # ---------------------------------------------------------------------------
    with Step("PerfDebug", "which records cost the most to process?",
              "per-record latency, attributed back to the input rows") as step:
        sample = spark.table("flights").limit(20000).coalesce(1)
        profile = bigasterisk.perfdebug(spark).profile(sample, top_k=3)
        analysis(profile.df, spark.table("carriers"), spark.table("airports")).collect()
        step.say("%d records profiled, skew %.1fx the mean" % (profile.records, profile.skew))
        for cost in profile.slowest[:3]:
            step.say("    %.3f ms  %s" % (cost.millis, _flight(cost.row)))
        step.finding("at this scale the top record is task warm-up rather than data; "
                     "the attribution is what is being shown")

    # ---------------------------------------------------------------------------
    # 10. Vega — the next revision
    # ---------------------------------------------------------------------------
    with Step("Vega", "how much of the fix can reuse the last run?",
              "match the revised plan against what the previous run materialised") as step:
        incremental = bigasterisk.vega(spark)
        try:
            step.say("running the original...")
            incremental.run(pipeline()).df.collect()
            # the fix: a guard against the malformed feed rows, applied to the input
            fixed = analysis(
                spark.table("flights").filter(col("arr_delay") < 10000),
                spark.table("carriers"), spark.table("airports"))
            step.say("running the fix (a guard against the malformed feed rows)...")
            revised = incremental.run(fixed)
            revised.df.collect()
            step.finding("reused %d of %d parts (%.0f%%)"
                         % (len(revised.reused), revised.steps, revised.reuse_ratio * 100))
            for part in revised.reused[:3]:
                step.say("    reused: %s" % part[:70])
        finally:
            incremental.clear()

    # ---------------------------------------------------------------------------
    # 11. BigTest and NaturalSym — inputs that drive each path, through the UDF
    # ---------------------------------------------------------------------------
    def long_hauls():
        """Long-haul flights — a filter whose whole condition lives inside a UDF."""
        return (spark.table("flights")
                .filter(haul(col("distance")) == "long")
                .select(col("flight_id")))

    with Step("BigTest", "what input drives each path, including inside the UDF?",
              "solve the query's branch conditions; the UDF's paths are now among them") as step:
        suite = bigasterisk.testgen(spark).generate(
            long_hauls(), {"flights": spark.table("flights")},
            rows_per_path=1, natural=False, seed=5)
        for case in suite.cases:
            step.say("%s" % case)
        step.finding("%d of %d generated inputs verified; the condition on the UDF's "
                     "result became a condition on `distance`"
                     % (len(suite.verified), len(suite.cases)))

    with Step("NaturalSym", "the same paths, but with values that look real",
              "witnesses drawn from values that occur, shaped by a declared distribution") as step:
        suite = bigasterisk.testgen(spark).generate(
            long_hauls(), {"flights": spark.table("flights")}, rows_per_path=1, natural=True,
            seed=5, distributions={"distance": "normal(1200, 600)"})
        for case in suite.cases:
            step.say("%s" % case)
        step.finding("same paths, records that read like records")

    # ---------------------------------------------------------------------------
    # 12. The three fuzzers
    # ---------------------------------------------------------------------------
    with Step("BigFuzz / DepFuzz / NaturalFuzz",
              "what else would break this pipeline?",
              "one query, three mutation strategies — the three papers differ in exactly "
              "where a generated value comes from") as step:
        # The seeds have to be the DataFrames the pipeline is built from: a campaign
        # substitutes generated rows into the plan at exactly those points.
        sample = spark.table("flights").limit(2000)
        carriers_table = spark.table("carriers")
        seeds = {"flights": sample, "carriers": carriers_table}

        late = (sample
                .filter(col("arr_delay") > 60)
                .join(carriers_table, col("carrier") == carriers_table["code"])
                .groupBy(carriers_table["name"])
                .agg(count("*").alias("n")))

        step.say("%-14s %10s %14s %10s" % ("strategy", "coverage", "empty results", "crashes"))
        for strategy in ("random", "natural", "co-dependent"):
            outcome = bigasterisk.fuzz(spark).fuzz(
                late, seeds, iterations=20, strategy=strategy, seed=1)
            step.say("%-14s %9.0f%% %10d/%-3d %10d"
                     % (strategy, outcome.coverage * 100, outcome.empty_results,
                        outcome.iterations, len(outcome.failures)))
        step.finding("a join key invented from nothing rarely matches, so most of the "
                     "random campaign is wasted; repairing the equality fixes it")

    print("\n" + "=" * 78)
    print("  Every tool ran. Each answered a different question about one fault.")
    print("=" * 78)
    return locals()


def _operation(operation):
    """One ranked operation, readably."""
    where = operation.branch if operation.branch else (operation.detail or "")
    return "[%s] %s %s" % (operation.step_id, operation.operator, where[:70])


def _quieten(spark, loggers, level="OFF"):
    """Set the level of named JVM loggers, for expected failures."""
    try:
        jvm = spark._jvm
        configurator = jvm.org.apache.logging.log4j.core.config.Configurator
        target = getattr(jvm.org.apache.logging.log4j.Level, level)
        for name in loggers:
            configurator.setLevel(name, target)
    except Exception:
        pass          # a different logging backend; the trace is noise, not a failure


def _flight(row):
    """One flight, readably.

    Witness rows carry only the columns a query touched, so a row that is missing the
    identifying fields is printed whole rather than as a row of Nones.
    """
    if hasattr(row, "asDict"):
        row = row.asDict()
    if not isinstance(row, dict):
        return str(row)
    if row.get("flight_id") is None:
        return ", ".join("%s=%s" % (k, v) for k, v in row.items() if v is not None)
    return "%s %s %s->%s arr_delay=%s" % (
        row.get("flight_id"), row.get("carrier"), row.get("origin"),
        row.get("dest"), row.get("arr_delay"))


if __name__ == "__main__":
    main()
