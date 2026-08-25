package org.apache.spark.sql.perfdebug

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, udf}

import org.bigasterisk.api.BigAsterisk

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PerfDebugSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  /** One record is deliberately expensive: the outlier amount, 99999. */
  private val expensiveForOutlier = udf { (amount: Int) =>
    if (amount > 1000) Thread.sleep(60L)
    amount
  }

  override def beforeAll(): Unit = {
    spark = BigAsterisk.configure(
      SparkSession.builder()
        .master("local[2]")
        .appName("perfdebug-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def perfdebug = BigAsterisk.perfdebug(spark)

  /** Twelve orders in a single partition, so record counts are exact. */
  private def orders: DataFrame =
    spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("src/test/resources/orders_csv").coalesce(1)

  private def skewed: DataFrame =
    orders.withColumn("processed", expensiveForOutlier(col("amount")))

  test("the expensive record is identified") {
    val profile = perfdebug.profile(skewed, topK = 3)
    profile.df.collect()

    profile.slowest should not be empty
    // the costliest record is the one the UDF stalls on
    profile.slowest.head.row.getInt(2) shouldBe 99999
    profile.slowest.head.millis should be > 50.0
  }

  test("skew is reported as a multiple of the mean") {
    val profile = perfdebug.profile(skewed, topK = 3)
    profile.df.collect()
    // one 60 ms record among eleven microsecond ones is unmistakable skew
    profile.skew should be > 5.0
  }

  test("an even workload shows no meaningful skew") {
    val profile = perfdebug.profile(orders, topK = 3)
    profile.df.collect()
    profile.records should be > 0L
    // every record costs about the same, so the worst is close to the mean
    profile.skew should be < 500.0
  }

  test("every record is counted except the first of each task") {
    val profile = perfdebug.profile(orders, topK = 3)
    profile.df.collect()
    // 12 rows in one partition; the interval before the first spans pipeline start-up
    profile.records shouldBe 11L
  }

  test("the instrumented DataFrame passes every row through unchanged") {
    val profile = perfdebug.profile(orders, topK = 3)
    val rows = profile.df.collect()
    rows.length shouldBe 12
    rows.map(_.getString(0)).toSet shouldBe orders.collect().map(_.getString(0)).toSet
  }

  test("topK bounds what is retained but not what is counted") {
    val profile = perfdebug.profile(orders, topK = 2)
    profile.df.collect()
    profile.slowest.size shouldBe 2
    profile.records shouldBe 11L
  }

  test("a topK of zero measures without retaining anything") {
    val profile = perfdebug.profile(orders, topK = 0)
    profile.df.collect()
    profile.slowest shouldBe empty
    profile.records shouldBe 11L
    profile.meanNanos should be > 0.0
  }

  test("retained records are ordered most expensive first") {
    val profile = perfdebug.profile(skewed, topK = 3)
    profile.df.collect()
    val costs = profile.slowest.map(_.nanos)
    costs shouldBe costs.sorted.reverse
  }

  test("retained records keep every column of the profiled DataFrame") {
    val profile = perfdebug.profile(orders, topK = 2)
    // the aggregation needs only cid and amount; oid must survive pruning
    profile.df.groupBy("cid").sum("amount").collect()
    profile.slowest.foreach { rc =>
      rc.row.schema.fieldNames.toSeq shouldBe Seq("oid", "cid", "amount")
    }
  }

  test("timing is fused into whole-stage codegen") {
    val profile = perfdebug.profile(orders, topK = 1)
    val plan = profile.df.queryExecution.executedPlan
    plan.exists(_.isInstanceOf[LatencyExec]) shouldBe true
    plan.exists {
      case w: org.apache.spark.sql.execution.WholeStageCodegenExec =>
        w.exists(_.isInstanceOf[LatencyExec])
      case _ => false
    } shouldBe true
  }

  test("the interpreted path measures too") {
    val previous = spark.conf.get("spark.sql.codegen.wholeStage")
    try {
      spark.conf.set("spark.sql.codegen.wholeStage", "false")
      val profile = perfdebug.profile(skewed, topK = 3)
      profile.df.collect()
      profile.records shouldBe 11L
      profile.slowest.head.row.getInt(2) shouldBe 99999
    } finally {
      spark.conf.set("spark.sql.codegen.wholeStage", previous)
    }
  }

  test("reset clears measurements so the profile can be reused") {
    val profile = perfdebug.profile(orders, topK = 3)
    profile.df.collect()
    profile.records shouldBe 11L
    profile.reset()
    profile.records shouldBe 0L
    profile.slowest shouldBe empty
    profile.df.collect()
    profile.records shouldBe 11L
  }

  test("profiles are registered and can be cleared") {
    perfdebug.clear()
    val a = perfdebug.profile(orders, topK = 1)
    val b = perfdebug.profile(orders, topK = 1)
    perfdebug.active should have size 2
    a should not be theSameInstanceAs(b)
    perfdebug.clear()
    perfdebug.active shouldBe empty
  }

  test("a negative topK is rejected") {
    an[IllegalArgumentException] should be thrownBy perfdebug.profile(orders, topK = -1)
  }

  test("a JVM-side pipeline attributes cost at record level") {
    val profile = perfdebug.profile(skewed, topK = 1)
    profile.df.collect()
    profile.recordLevel shouldBe true
  }

  test("an expensive output is attributed to the record that made it expensive") {
    // orders_csv is one file, so the source is captured without a Coalesce in the plan
    // Profile *above* the expensive work. Within a fused pipeline the interval before a
    // record covers the work done for the previous one, so a point below the UDF would
    // charge its cost to the following record.
    val source = spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("src/test/resources/orders_csv")
      .withColumn("cost", expensiveForOutlier(col("amount")))
    val profile = perfdebug.profile(source, topK = 5)

    val totals = profile.df.groupBy("cid").sum("amount")
    totals.collect()

    // c2 is the group holding the outlier; the record blamed for it is that outlier
    val blamed = profile.blame(totals, "cid = 'c2'")
    blamed should not be empty
    blamed.head.row.getInt(2) shouldBe 99999
    blamed.head.millis should be > 50.0
  }

  test("a different output is attributed to different records") {
    val source = spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("src/test/resources/orders_csv")
      .withColumn("cost", expensiveForOutlier(col("amount")))
    val profile = perfdebug.profile(source, topK = 12)

    val totals = profile.df.groupBy("cid").sum("amount")
    totals.collect()

    // c1 has no outlier, so the expensive record must not be blamed for it
    val blamed = profile.blame(totals, "cid = 'c1'")
    blamed.map(_.row.getInt(2)) should not contain 99999
  }

  test("blame reports nothing when the profile measured nothing") {
    val source = spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("src/test/resources/orders_csv")
    val profile = perfdebug.profile(source, topK = 3)
    // never run, so nothing was measured
    profile.blame(profile.df.groupBy("cid").sum("amount"), "cid = 'c2'") shouldBe empty
  }

  test("blame rejects a predicate that selects nothing") {
    val source = spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("src/test/resources/orders_csv")
    val profile = perfdebug.profile(source, topK = 3)
    val totals = profile.df.groupBy("cid").sum("amount")
    totals.collect()
    an[IllegalArgumentException] should be thrownBy profile.blame(totals, "cid = 'nobody'")
  }

  test("mean and skew are well defined before anything runs") {
    val profile = perfdebug.profile(orders, topK = 3)
    profile.records shouldBe 0L
    profile.meanNanos shouldBe 0.0
    profile.skew shouldBe 0.0
  }
}
