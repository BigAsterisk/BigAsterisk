package org.bigasterisk.examples

import scala.util.control.NonFatal

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col

import org.bigasterisk.api._
import org.apache.spark.sql.bigsift.BigSiftSQL

import org.bigasterisk.optdebug.OptDebug

/**
 * Every tool in the platform, on one small dataset, in one run.
 *
 * ==What it is for==
 * The documentation explains each tool separately. This shows them working together on
 * a query with a planted fault, which is the only way to see that they answer different
 * questions about the same failure:
 *
 *   - which records produced this wrong answer                (Titian)
 *   - which of them actually mattered                         (FlowDebug)
 *   - the minimal input that still reproduces it              (BigSift, in the data)
 *   - which operation mishandled it                           (OptDebug, in the code)
 *   - which record was in flight when it crashed              (BigDebug)
 *   - which record cost too much                              (PerfDebug)
 *   - what else would break it                                (fuzzing, test generation)
 *   - and how to not pay for the same work twice              (Vega)
 *
 * ==Running it==
 * {{{
 * bin/bigasterisk tour
 * }}}
 *
 * It exits non-zero if any section fails, so it doubles as an end-to-end check that the
 * tools compose — the unit suites each exercise one tool in isolation.
 */
object PlatformTour {

  /** The planted fault: amounts over 1000 are negated, which only the outlier hits. */
  private val FaultyQuery =
    """SELECT cid, SUM(CASE WHEN amount > 1000 THEN -amount ELSE amount END) AS total
      |FROM orders GROUP BY cid""".stripMargin

  private var failures = 0

  def main(args: Array[String]): Unit = {
    val spark = BigAsterisk
      .configure(
        SparkSession.builder()
          .master("local[2]")
          .appName("BigAsterisk platform tour")
          .config("spark.ui.enabled", "false")
          .config("spark.sql.shuffle.partitions", "2")
          .config("spark.task.maxFailures", "1"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val orders = spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("examples/data/orders.txt")
    // Coalescing puts a CoalesceExec in the plan, which is outside the capture engine's
    // verified operator set — so it is applied only in the two sections that want a
    // single partition for the record index to mean something, neither of which
    // captures lineage.
    val singlePartition = orders.coalesce(1)
    val customers = spark.read.schema("cid STRING, name STRING")
      .csv("examples/data/customers.txt")
    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")

    println(s"Spark ${spark.version}, binding '${BigAsterisk.binding(spark).name}'")
    println(s"${orders.count()} orders, ${customers.count()} customers. " +
      "One order is an outlier: o8, 99999.\n")

    section("DeSQL — step through the query") {
      BigAsterisk.desql(spark).decompose(spark, FaultyQuery).foreach { step =>
        println(f"  [${step.id}] ${step.operator}%-12s ${step.detail}")
      }
    }

    section("Titian — which records produced the wrong total") {
      val lineage = BigAsterisk.lineage(spark)
      lineage.enableCapture(spark)
      try {
        val df = spark.sql(FaultyQuery)
        val out = lineage.collectWithLineage(df)
        val faulty = out.find { case (r, _) => r.getLong(1) < 0 }.get
        println(s"  faulty output: ${faulty._1}")
        val witnesses = lineage.showInputs(df, lineage.backward(df, Seq(faulty._2)).toSeq)
        println(s"  provenance returns ${witnesses.length} records: " +
          witnesses.map(_.toString).mkString(", "))
        lineage.releaseLineage(df)
      } finally lineage.disableCapture(spark)
    }

    section("FlowDebug — which of them actually mattered") {
      BigAsterisk.influence(spark)
        .influencers(spark.sql(FaultyQuery), "total < 0", topK = 4)
        .foreach(i => println(s"  $i"))
    }

    section("BigSift — which input records are to blame (data-space)") {
      val result = BigSiftSQL.debug(spark, "orders", FaultyQuery, (r: Row) => r.getLong(1) < 0)
      println(s"  provenance left ${result.provenanceSize} candidate records; " +
        s"delta debugging narrowed them to ${result.faultInducingRows.size}")
      result.faultInducingRows.foreach(r => println(s"    $r"))
    }

    section("OptDebug — which operation is to blame (code-space)") {
      // Fault isolation in the code: which operation is responsible, as opposed to
      // which input records are.
      OptDebug.localize(spark, "orders", FaultyQuery, (r: Row) => r.getLong(1) < 0)
        .ranked.take(3).foreach(op => println(s"  $op"))
    }

    section("BigDebug — a breakpoint: the state at a point, without pausing") {
      val bp = BigAsterisk.breakpoints(spark).breakpoint(orders.filter(col("amount") > 300))
      // the query runs at full speed through it; the state is regenerated only now
      bp.df.groupBy("cid").sum("amount").collect()
      println(s"  ${bp.count()} records were flowing past; the first few:")
      bp.state(limit = 3).foreach(r => println(s"    $r"))
    }

    section("BigDebug — a watchpoint on the records flowing past") {
      val wp = BigAsterisk.watchpoints(spark).watch(orders, col("amount") > 1000)
      wp.df.groupBy("cid").sum("amount").collect()
      println(s"  ${wp.hits} record(s) matched ${wp.condition}")
      wp.captured.foreach(r => println(s"    $r"))
    }

    section("BigDebug — which record killed the query") {
      val guard = BigAsterisk.crashCulprit(spark).guard(singlePartition)
      spark.conf.set("spark.sql.ansi.enabled", "true")
      // the failure is expected, so keep Spark's task-failure stack traces out of the way
      spark.sparkContext.setLogLevel("OFF")
      try {
        guard.df.selectExpr("oid", "100 DIV (amount - 99999) AS boom").collect()
        println("  (expected a failure and did not get one)")
      } catch {
        case NonFatal(_) => guard.culprit.foreach(c => println(s"  $c"))
      } finally {
        spark.sparkContext.setLogLevel("ERROR")
        spark.conf.set("spark.sql.ansi.enabled", "false")
      }
    }

    section("PerfDebug — which record cost too much") {
      val profile = BigAsterisk.perfdebug(spark).profile(singlePartition, topK = 3)
      profile.df.groupBy("cid").sum("amount").collect()
      println(f"  ${profile.records} records, skew ${profile.skew}%.1fx the mean")
      profile.slowest.take(2).foreach(r => println(s"    $r"))
    }

    section("Vega — the next revision reuses what it can") {
      val vega = BigAsterisk.vega(spark)
      try {
        vega.run(spark.sql("SELECT cid, amount FROM orders WHERE amount > 100")).df.collect()
        val revised = vega.run(spark.sql(
          "SELECT cid, SUM(amount) AS total FROM orders WHERE amount > 100 GROUP BY cid"))
        println(f"  reused ${revised.reused.size} of ${revised.steps} parts " +
          f"(${revised.reuseRatio * 100}%.0f%%): ${revised.reused.mkString(", ")}")
        revised.df.collect()
      } finally vega.clear()
    }

    section("Fuzzing — what else would break it") {
      val result = BigAsterisk.fuzz(spark).fuzz(
        "SELECT c.name, SUM(o.amount) AS total FROM orders o " +
          "JOIN customers c ON o.cid = c.cid WHERE o.amount > 100 GROUP BY c.name",
        Map("orders" -> orders, "customers" -> customers),
        FuzzConfig(iterations = 20, seed = 1L))
      println(s"  $result")
      result.failures.take(1).foreach(f => println(s"    $f"))
    }

    section("BigTest — an input per path through the query") {
      BigAsterisk.testgen(spark)
        .generate("SELECT cid FROM orders WHERE amount > 100", Map("orders" -> orders),
          TestGenConfig(rowsPerPath = 1))
        .cases.foreach(c => println(s"  $c"))
    }

    spark.stop()

    println()
    if (failures == 0) {
      println("TOUR OK — every tool ran")
    } else {
      println(s"TOUR FAILED — $failures section(s) failed")
      System.exit(1)
    }
  }

  /** Runs one section, reporting rather than aborting so the whole tour is seen. */
  private def section(title: String)(body: => Unit): Unit = {
    println(s"── $title")
    try {
      body
      println()
    } catch {
      case NonFatal(e) =>
        failures += 1
        println(s"  FAILED: ${e.getClass.getSimpleName}: ${e.getMessage}\n")
    }
  }
}
