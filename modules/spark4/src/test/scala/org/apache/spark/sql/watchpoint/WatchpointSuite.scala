package org.apache.spark.sql.watchpoint

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import org.bigasterisk.api.BigAsterisk

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class WatchpointSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  // amounts in orders_csv: 420 250 80 310 190 95 60 99999 75 110 205 380
  private val overThreeHundred = Set(420, 310, 99999, 380)

  override def beforeAll(): Unit = {
    spark = BigAsterisk.configure(
      SparkSession.builder()
        .master("local[2]")
        .appName("watchpoint-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("src/test/resources/orders_csv").createOrReplaceTempView("orders")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def watchpoints = BigAsterisk.watchpoints(spark)
  private def orders = spark.table("orders")

  test("counts the rows matching the guard") {
    val wp = watchpoints.watch(orders, col("amount") > 300)
    wp.df.collect()
    wp.hits shouldBe 4L
  }

  test("captures the matching rows, not a repeated buffer") {
    val wp = watchpoints.watch(orders, col("amount") > 300)
    wp.df.collect()
    val amounts = wp.captured.map(_.getInt(2))
    // the UnsafeRow handed to the accumulator is a reusable buffer; without a defensive
    // copy every captured row would be the last match repeated
    amounts.toSet shouldBe overThreeHundred
    amounts.length shouldBe 4
    wp.truncated shouldBe false
  }

  test("the instrumented DataFrame passes every row through unchanged") {
    val wp = watchpoints.watch(orders, col("amount") > 300)
    val observed = wp.df.collect()
    observed.length shouldBe 12
    observed.map(_.getString(0)).toSet shouldBe orders.collect().map(_.getString(0)).toSet
  }

  test("capacity bounds what comes back but not what is counted") {
    val wp = watchpoints.watch(orders, col("amount") > 100, capacity = 3)
    wp.df.collect()
    wp.hits shouldBe 8L                 // every matching row is counted
    wp.captured.length shouldBe 3       // only three are shipped to the driver
    wp.truncated shouldBe true
    wp.capacity shouldBe 3
  }

  test("a capacity of zero counts without retaining anything") {
    val wp = watchpoints.watch(orders, col("amount") > 100, capacity = 0)
    wp.df.collect()
    wp.hits shouldBe 8L
    wp.captured shouldBe empty
    wp.truncated shouldBe true
  }

  test("a guard that matches nothing reports nothing") {
    val wp = watchpoints.watch(orders, col("amount") > 1000000)
    wp.df.collect()
    wp.hits shouldBe 0L
    wp.captured shouldBe empty
    wp.truncated shouldBe false
  }

  test("the guard is fused into whole-stage codegen") {
    val wp = watchpoints.watch(orders, col("amount") > 300)
    val plan = wp.df.queryExecution.executedPlan
    plan.exists(_.isInstanceOf[WatchpointExec]) shouldBe true
    // the operator implements CodegenSupport, so it must sit inside a codegen stage
    plan.exists {
      case w: org.apache.spark.sql.execution.WholeStageCodegenExec =>
        w.exists(_.isInstanceOf[WatchpointExec])
      case _ => false
    } shouldBe true
  }

  test("the interpreted path agrees with the codegen path") {
    val previous = spark.conf.get("spark.sql.codegen.wholeStage")
    try {
      spark.conf.set("spark.sql.codegen.wholeStage", "false")
      val wp = watchpoints.watch(orders, col("amount") > 300)
      wp.df.collect()
      wp.hits shouldBe 4L
      wp.captured.map(_.getInt(2)).toSet shouldBe overThreeHundred
    } finally {
      spark.conf.set("spark.sql.codegen.wholeStage", previous)
    }
  }

  test("observations survive a downstream shuffle") {
    val wp = watchpoints.watch(orders, col("amount") > 300)
    // the watchpoint sits below an aggregation, so its rows cross an exchange
    wp.df.groupBy("cid").sum("amount").collect()
    wp.hits shouldBe 4L
    wp.captured.map(_.getInt(2)).toSet shouldBe overThreeHundred
  }

  test("captured rows keep every column of the watched DataFrame") {
    val wp = watchpoints.watch(orders, col("amount") > 300)
    // the aggregation needs only cid and amount, so column pruning would drop oid
    wp.df.groupBy("cid").sum("amount").collect()
    wp.captured should not be empty
    wp.captured.foreach { r =>
      r.length shouldBe 3
      r.getString(0) should startWith("o")   // oid survived pruning
    }
    wp.captured.map(_.schema.fieldNames.toSeq).distinct shouldBe
      Seq(Seq("oid", "cid", "amount"))
  }

  test("reset clears observations so the watchpoint can be reused") {
    val wp = watchpoints.watch(orders, col("amount") > 300)
    wp.df.collect()
    wp.hits shouldBe 4L
    wp.reset()
    wp.hits shouldBe 0L
    wp.captured shouldBe empty
    wp.df.collect()
    wp.hits shouldBe 4L
  }

  test("re-running without a reset accumulates, as accumulators do") {
    val wp = watchpoints.watch(orders, col("amount") > 300)
    wp.df.collect()
    wp.df.collect()
    wp.hits shouldBe 8L
  }

  test("the condition is reported as SQL text") {
    val wp = watchpoints.watch(orders, col("amount") > 300)
    wp.condition should include("amount")
    wp.condition should include("300")
  }

  test("watchpoints are registered and can be cleared") {
    watchpoints.clear()
    val a = watchpoints.watch(orders, col("amount") > 300)
    val b = watchpoints.watch(orders, col("amount") > 100)
    watchpoints.active.map(_.id) should contain allOf (a.id, b.id)
    a.id should not be b.id
    watchpoints.clear()
    watchpoints.active shouldBe empty
  }

  test("two watchpoints on one query observe independently") {
    val big = watchpoints.watch(orders, col("amount") > 300)
    val small = watchpoints.watch(big.df, col("amount") < 100)
    small.df.collect()
    big.hits shouldBe 4L      // 420 310 99999 380
    small.hits shouldBe 4L    // 80 95 60 75
  }

  test("a negative capacity is rejected") {
    an[IllegalArgumentException] should be thrownBy
      watchpoints.watch(orders, col("amount") > 300, capacity = -1)
  }

  test("a watchpoint does not break lineage capture") {
    val lineage = BigAsterisk.lineage(spark)
    lineage.enableCapture(spark)
    try {
      val wp = watchpoints.watch(orders, col("amount") > 300)
      val df = wp.df.groupBy("cid").sum("amount")
      // Titian's coverage check must accept the watchpoint as a pass-through rather
      // than aborting the query
      val out = lineage.collectWithLineage(df)
      out should not be empty
      wp.hits shouldBe 4L
      lineage.releaseLineage(df)
    } finally {
      lineage.disableCapture(spark)
    }
  }
}
