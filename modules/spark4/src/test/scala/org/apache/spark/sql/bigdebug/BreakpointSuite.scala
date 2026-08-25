package org.apache.spark.sql.bigdebug

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

import org.bigasterisk.api.BigAsterisk

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BreakpointSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = BigAsterisk.configure(
      SparkSession.builder()
        .master("local[2]")
        .appName("breakpoint-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def breakpoints = BigAsterisk.breakpoints(spark)

  /** amounts: 420 250 80 310 190 95 60 99999 75 110 205 380 */
  private def orders: DataFrame =
    spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("src/test/resources/orders_csv")

  test("the state at a breakpoint is the records flowing past it") {
    val bp = breakpoints.breakpoint(orders.filter(col("amount") > 100))
    val state = bp.state()
    state.length shouldBe 8
    state.map(_.getInt(2)).foreach(_ should be > 100)
  }

  test("setting one does not change the query it is set on") {
    val filtered = orders.filter(col("amount") > 100)
    val bp = breakpoints.breakpoint(filtered)
    bp.df.collect() shouldBe filtered.collect()
  }

  test("the query runs through the breakpoint unchanged") {
    val bp = breakpoints.breakpoint(orders.filter(col("amount") > 100))
    val downstream = bp.df.groupBy("cid").sum("amount").collect()
    val expected = orders.filter(col("amount") > 100).groupBy("cid").sum("amount").collect()
    downstream.map(r => (r.getString(0), r.getLong(1))).toSet shouldBe
      expected.map(r => (r.getString(0), r.getLong(1))).toSet
  }

  test("no operator is inserted into the plan") {
    // the whole point: a breakpoint records where to look, it does not instrument
    val filtered = orders.filter(col("amount") > 100)
    val bp = breakpoints.breakpoint(filtered)
    bp.df.queryExecution.executedPlan.toString shouldBe
      filtered.queryExecution.executedPlan.toString
  }

  test("the shape of the state is known without computing it") {
    val bp = breakpoints.breakpoint(orders.filter(col("amount") > 100))
    bp.schema.fieldNames.toSeq shouldBe Seq("oid", "cid", "amount")
    bp.isMaterialized shouldBe false
  }

  test("state is bounded by the requested limit") {
    val bp = breakpoints.breakpoint(orders)
    bp.state(limit = 3).length shouldBe 3
    bp.state(limit = 0) shouldBe empty
    bp.count() shouldBe 12L
  }

  test("a negative limit is rejected") {
    val bp = breakpoints.breakpoint(orders)
    an[IllegalArgumentException] should be thrownBy bp.state(limit = -1)
  }

  test("materializing pins the state, and releasing unpins it") {
    val bp = breakpoints.breakpoint(orders.filter(col("amount") > 100))
    bp.isMaterialized shouldBe false

    bp.materialize()
    bp.isMaterialized shouldBe true
    // the pinned state answers the same as the recomputed one
    bp.state().map(_.getString(0)).toSet shouldBe
      orders.filter(col("amount") > 100).collect().map(_.getString(0)).toSet

    bp.materialize() // idempotent
    bp.isMaterialized shouldBe true

    bp.release()
    bp.isMaterialized shouldBe false
    bp.release() // idempotent
    bp.isMaterialized shouldBe false
  }

  test("execution can be resumed from the breakpoint") {
    val bp = breakpoints.breakpoint(orders.filter(col("amount") > 100))
    val resumed = bp.resumeWith(_.groupBy("cid").sum("amount"))
    val expected = orders.filter(col("amount") > 100).groupBy("cid").sum("amount")
    resumed.collect().map(r => (r.getString(0), r.getLong(1))).toSet shouldBe
      expected.collect().map(r => (r.getString(0), r.getLong(1))).toSet
  }

  test("resuming with a corrected step re-runs only from the breakpoint") {
    val bp = breakpoints.breakpoint(orders)
    bp.materialize()
    try {
      // the step after the breakpoint was wrong; fix it and resume without re-reading
      // the source
      val corrected = bp.resumeWith(_.filter(col("amount") <= 1000).groupBy("cid").sum("amount"))
      val totals = corrected.collect().map(r => (r.getString(0), r.getLong(1))).toMap
      // c2's outlier is excluded by the corrected step
      totals("c2") shouldBe 645L
    } finally bp.release()
  }

  test("breakpoints are registered, and clearing releases what they pinned") {
    breakpoints.clear()
    val a = breakpoints.breakpoint(orders)
    val b = breakpoints.breakpoint(orders.filter(col("amount") > 100))
    a.materialize()
    breakpoints.active.map(_.id) should contain allOf (a.id, b.id)
    a.id should not be b.id

    breakpoints.clear()
    breakpoints.active shouldBe empty
    a.isMaterialized shouldBe false
  }

  test("toString names the state's shape") {
    val bp = breakpoints.breakpoint(orders)
    bp.toString should include("oid")
    bp.materialize()
    try bp.toString should include("materialized")
    finally bp.release()
  }
}
