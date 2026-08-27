package org.bigasterisk.benchmarks

import java.io.{File, PrintWriter}

import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession

import org.bigasterisk.api._
import org.bigasterisk.optdebug.OptDebug

/**
 * What one tool did on one benchmark.
 *
 * @param metric the number the tool's own evaluation reports, where it has one
 * @param detail how it was obtained, so a number is never quoted without its meaning
 * @param status `ok`, `n/a` when the benchmark carries no fault model the tool needs,
 *               or `error` with the reason
 */
case class Measurement(
    benchmark: String,
    tool: String,
    metric: String,
    detail: String,
    status: String = "ok") {

  def isOk: Boolean = status == "ok"
}

/**
 * Runs the tools over the published subject programs and reports what each found.
 *
 * ==What this is, and is not==
 * It is a harness: every tool, over the programs the papers evaluated on, producing the
 * measurement each tool's own evaluation reports — coverage, the size of an isolated
 * input set, the rank of a faulty operation. Running it says whether the platform works
 * on the workloads the claims were made about.
 *
 * It is **not** a reproduction of the papers' results. Reproducing a speedup or a
 * precision *ratio* needs the baseline each paper compared against — plain provenance,
 * unguided generation, random mutation — and none of those is implemented here. The
 * numbers below stand on their own; they are not differences against anything.
 *
 * {{{
 * bin/sbt 'benchmarks/runMain org.bigasterisk.benchmarks.BenchmarkRunner'
 * bin/sbt 'benchmarks/runMain org.bigasterisk.benchmarks.BenchmarkRunner CommuteType'
 * }}}
 */
object BenchmarkRunner {

  /** Rows per generated table. Small: these measure behaviour, not throughput. */
  private val DefaultRows = 400

  def main(args: Array[String]): Unit = {
    val selected =
      if (args.isEmpty) Benchmark.all
      else args.flatMap(name => Benchmark.byName(name)).toSeq

    if (selected.isEmpty) {
      System.err.println(
        s"No such benchmark. Known: ${Benchmark.all.map(_.name).mkString(", ")}")
      System.exit(2)
    }

    val spark = BigAsterisk.configure(
      SparkSession.builder()
        .master(sys.env.getOrElse("BENCHMARK_MASTER", "local[2]"))
        .appName("BigAsterisk benchmarks")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try {
      val measurements = selected.flatMap { benchmark =>
        println(s"── ${benchmark.name}  (${benchmark.papers.mkString(", ")})")
        val results = measure(spark, benchmark)
        results.foreach(m => println(f"     ${m.tool}%-12s ${m.metric}%-22s ${m.detail}"))
        results
      }

      report(measurements)
    } finally {
      spark.stop()
    }
  }

  /** Every applicable tool, over one benchmark. */
  def measure(spark: SparkSession, benchmark: Benchmark): Seq[Measurement] = Seq(
    desql(spark, benchmark),
    testgen(spark, benchmark),
    fuzz(spark, benchmark),
    titian(spark, benchmark),
    bigsift(spark, benchmark),
    optdebug(spark, benchmark),
    flowdebug(spark, benchmark))

  // ---------------------------------------------------------------------------
  // One measurement per tool. Each reports the quantity that tool's own evaluation
  // reports, and says `n/a` rather than inventing a fault model the benchmark lacks.
  // ---------------------------------------------------------------------------

  private def desql(spark: SparkSession, b: Benchmark): Measurement =
    attempt(b, "DeSQL") {
      b.register(spark, DefaultRows)
      val steps = BigAsterisk.desql(spark).decompose(spark.sql(b.query))
      Measurement(b.name, "DeSQL", s"${steps.size} steps",
        s"${steps.count(_.branches.nonEmpty)} carry branches")
    }

  private def testgen(spark: SparkSession, b: Benchmark): Measurement =
    attempt(b, "BigTest") {
      val seeds = b.register(spark, DefaultRows)
      val suite = BigAsterisk.testgen(spark).generate(b.query, seeds, TestGenConfig(seed = 1L))
      Measurement(b.name, "BigTest", f"${suite.coverage * 100}%.0f%% branch coverage",
        s"${suite.verified.size}/${suite.cases.size} generated inputs verified, " +
          s"${suite.totalBranches} branches")
    }

  private def fuzz(spark: SparkSession, b: Benchmark): Measurement =
    attempt(b, "Fuzzing") {
      val seeds = b.register(spark, DefaultRows)
      val result = BigAsterisk.fuzz(spark).fuzz(
        b.query, seeds, FuzzConfig(iterations = 30, seed = 1L))
      Measurement(b.name, "Fuzzing", f"${result.coverage * 100}%.0f%% branch coverage",
        s"${result.failures.size} crash(es) in ${result.iterations} iterations, " +
          s"${result.abstracted} without Spark")
    }

  private def titian(spark: SparkSession, b: Benchmark): Measurement =
    attempt(b, "Titian") {
      b.register(spark, DefaultRows)
      val lineage = BigAsterisk.lineage(spark)
      lineage.enableCapture(spark)
      try {
        val df = spark.sql(b.query)
        val outputs = lineage.collectWithLineage(df)
        if (outputs.isEmpty) {
          Measurement(b.name, "Titian", "no output", "the query returned nothing", "n/a")
        } else {
          // walk back to the scan: the witnesses of one output row are the source
          // records the tracer reaches, which is the provenance size the paper reports
          var cursor = lineage.trace(df, Seq(outputs.head._2))
          while (!cursor.atScan) cursor = cursor.goBack()
          Measurement(b.name, "Titian", s"${cursor.show().length} witnesses",
            s"for 1 of ${outputs.length} output rows")
        }
      } finally {
        lineage.disableCapture(spark)
      }
    }

  private def bigsift(spark: SparkSession, b: Benchmark): Measurement =
    if (b.corrupt.isEmpty || b.oracle.isEmpty) {
      Measurement(b.name, "BigSift", "—",
        "the upstream evaluation plants no corrupt record for this program", "n/a")
    } else attempt(b, "BigSift") {
      val tables = b.register(spark, DefaultRows, withCorrupt = true)
      val table = b.corrupt.get.keys.head
      val total = tables(table).count()
      // the oracle is written once as SQL and compiled, so the predicate the other
      // tools evaluate and the function this one calls cannot mean different things
      val test = Oracle.compile(spark, b.query, b.oracle.get)
      val result = org.apache.spark.sql.bigsift.BigSiftSQL.debug(spark, table, b.query, test)
      Measurement(b.name, "BigSift", s"${result.faultInducingRows.size} of $total records",
        s"provenance left ${result.provenanceSize}")
    }

  private def optdebug(spark: SparkSession, b: Benchmark): Measurement =
    if (b.faulty.isEmpty) {
      Measurement(b.name, "OptDebug", "—",
        "the upstream evaluation injects no program fault for this program", "n/a")
    } else attempt(b, "OptDebug") {
      b.register(spark, DefaultRows)
      // wrongness by difference from the correct program: a per-program threshold would
      // need recalibrating for every input size, and would quietly stop testing anything
      val wrong = Oracle.differential(spark, b.query)
      val result = OptDebug.localize(spark, spark.sql(b.faulty.get), wrong)
      val prime = result.prime
      Measurement(b.name, "OptDebug",
        prime.map(p => f"top score ${p.score}%.2f").getOrElse("nothing ranked"),
        prime.map(p => s"${p.operator} ${p.branch.getOrElse(p.detail)}").getOrElse("—"))
    }

  private def flowdebug(spark: SparkSession, b: Benchmark): Measurement =
    if (b.oracle.isEmpty) {
      Measurement(b.name, "FlowDebug", "—", "no oracle for this program", "n/a")
    } else attempt(b, "FlowDebug") {
      b.register(spark, DefaultRows, withCorrupt = b.corrupt.isDefined)
      val query = if (b.corrupt.isDefined) b.query else b.faulty.getOrElse(b.query)
      val ranked = BigAsterisk.influence(spark).influencers(spark.sql(query), b.oracle.get)
      val top = ranked.headOption
      Measurement(b.name, "FlowDebug",
        top.map(t => f"${t.score}%.2f top influence").getOrElse("nothing ranked"),
        s"${ranked.size} ranked; ${top.map(_.reason).getOrElse("—")}")
    }

  // ---------------------------------------------------------------------------

  /** Runs a measurement, turning a failure into a reported row rather than a crash. */
  private def attempt(b: Benchmark, tool: String)(body: => Measurement): Measurement =
    try body
    catch {
      case NonFatal(e) =>
        Measurement(b.name, tool, "error",
          s"${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("")
            .split("\n").head.take(120)}", "error")
    }

  /** Writes the results as markdown and as CSV, and prints the summary. */
  private def report(measurements: Seq[Measurement]): Unit = {
    val tools = measurements.map(_.tool).distinct
    val byBenchmark = measurements.groupBy(_.benchmark)
    val order = Benchmark.all.map(_.name).filter(byBenchmark.contains)

    val markdown = new StringBuilder
    markdown ++= s"| Program | Papers | ${tools.mkString(" | ")} |\n"
    markdown ++= s"|---|---|${tools.map(_ => "---").mkString("|")}|\n"
    order.foreach { name =>
      val benchmark = Benchmark.byName(name).get
      val cells = tools.map { tool =>
        byBenchmark(name).find(_.tool == tool) match {
          case Some(m) if m.isOk          => m.metric
          case Some(m) if m.status == "n/a" => "—"
          case Some(m)                    => s"**${m.status}**"
          case None                       => ""
        }
      }
      markdown ++= s"| ${benchmark.name} | ${benchmark.papers.mkString(", ")} | " +
        s"${cells.mkString(" | ")} |\n"
    }

    val out = new File("benchmarks-results.md")
    val writer = new PrintWriter(out)
    try {
      writer.println("# Benchmark results")
      writer.println()
      writer.println("Generated by `benchmarks/runMain org.bigasterisk.benchmarks.BenchmarkRunner`.")
      writer.println("Numbers stand on their own: no baseline is implemented, so none of")
      writer.println("these is a ratio against another technique.")
      writer.println()
      writer.print(markdown)
      writer.println()
      writer.println("## Detail")
      writer.println()
      writer.println("| Program | Tool | Result | How | Status |")
      writer.println("|---|---|---|---|---|")
      measurements.foreach { m =>
        writer.println(s"| ${m.benchmark} | ${m.tool} | ${m.metric} | ${m.detail} | ${m.status} |")
      }
    } finally writer.close()

    val csv = new PrintWriter(new File("benchmarks-results.csv"))
    try {
      csv.println("benchmark,tool,metric,detail,status")
      measurements.foreach { m =>
        def quote(s: String) = "\"" + s.replace("\"", "\"\"") + "\""
        csv.println(s"${m.benchmark},${m.tool},${quote(m.metric)},${quote(m.detail)},${m.status}")
      }
    } finally csv.close()

    val errors = measurements.count(_.status == "error")
    println()
    print(markdown)
    println()
    println(s"${measurements.count(_.isOk)} measurements, " +
      s"${measurements.count(_.status == "n/a")} not applicable, $errors errors")
    println(s"wrote ${out.getName} and benchmarks-results.csv")

    if (errors > 0) System.exit(1)
  }
}
