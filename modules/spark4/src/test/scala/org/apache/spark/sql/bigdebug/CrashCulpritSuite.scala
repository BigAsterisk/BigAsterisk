package org.apache.spark.sql.bigdebug

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, udf}

import org.bigasterisk.api.BigAsterisk

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CrashCulpritSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  /** Fails on exactly one record: the outlier amount, 99999. */
  private val explodesOnOutlier = udf { (amount: Int) =>
    if (amount > 1000) throw new IllegalStateException(s"cannot handle $amount")
    amount
  }

  override def beforeAll(): Unit = {
    spark = BigAsterisk.configure(
      SparkSession.builder()
        .master("local[2]")
        .appName("crashculprit-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        // one attempt: retries would work, but the suite should not wait for them
        .config("spark.task.maxFailures", "1")).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def guards = BigAsterisk.crashCulprit(spark)

  /** Twelve orders in one partition, so the record index is meaningful. */
  private def orders: DataFrame =
    spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("src/test/resources/orders_csv").coalesce(1)

  private def runAndCatch(df: DataFrame): Unit =
    try df.collect()
    catch { case _: Throwable => () }

  test("the record that killed the query is named") {
    val guard = guards.guard(orders)
    runAndCatch(guard.df.withColumn("boom", explodesOnOutlier(col("amount"))))

    guard.culprit shouldBe defined
    guard.culprit.get.row.getInt(2) shouldBe 99999
    guard.culprit.get.row.getString(0) shouldBe "o8"
  }

  test("the failure is reported with its partition and position") {
    val guard = guards.guard(orders)
    runAndCatch(guard.df.withColumn("boom", explodesOnOutlier(col("amount"))))

    val culprit = guard.culprit.get
    culprit.partitionId shouldBe 0
    // o8 is the eighth of twelve rows, so index 7
    culprit.recordIndex shouldBe 7L
    culprit.error should include("cannot handle 99999")
  }

  test("the culprit keeps every column of the guarded DataFrame") {
    val guard = guards.guard(orders)
    // the projection needs only `amount`; the guard must hold on to the rest
    runAndCatch(guard.df.select(explodesOnOutlier(col("amount")).as("boom")))
    guard.culprit.get.row.schema.fieldNames.toSeq shouldBe Seq("oid", "cid", "amount")
  }

  test("a query that succeeds reports no culprit") {
    val guard = guards.guard(orders)
    guard.df.collect()
    guard.culprit shouldBe empty
  }

  test("the guarded DataFrame passes every row through unchanged") {
    val guard = guards.guard(orders)
    val rows = guard.df.collect()
    rows.length shouldBe 12
    rows.map(_.getString(0)).toSet shouldBe orders.collect().map(_.getString(0)).toSet
  }

  test("the guard is fused into whole-stage codegen") {
    val guard = guards.guard(orders)
    val plan = guard.df.queryExecution.executedPlan
    plan.exists(_.isInstanceOf[CrashCulpritExec]) shouldBe true
    plan.exists {
      case w: org.apache.spark.sql.execution.WholeStageCodegenExec =>
        w.exists(_.isInstanceOf[CrashCulpritExec])
      case _ => false
    } shouldBe true
  }

  test("the interpreted path names the same record") {
    val previous = spark.conf.get("spark.sql.codegen.wholeStage")
    try {
      spark.conf.set("spark.sql.codegen.wholeStage", "false")
      val guard = guards.guard(orders)
      runAndCatch(guard.df.withColumn("boom", explodesOnOutlier(col("amount"))))
      guard.culprit.get.row.getInt(2) shouldBe 99999
    } finally {
      spark.conf.set("spark.sql.codegen.wholeStage", previous)
    }
  }

  test("a failure downstream of the guard is still attributed") {
    val guard = guards.guard(orders)
    // the guard sits below a filter and a projection; the exception is thrown above both
    runAndCatch(
      guard.df.filter(col("amount") > 0).withColumn("boom", explodesOnOutlier(col("amount"))))
    guard.culprit.get.row.getInt(2) shouldBe 99999
  }

  test("reset clears the report so the guard can be reused") {
    val guard = guards.guard(orders)
    runAndCatch(guard.df.withColumn("boom", explodesOnOutlier(col("amount"))))
    guard.culprit shouldBe defined
    guard.reset()
    guard.culprit shouldBe empty
  }

  test("guards are registered and can be cleared") {
    guards.clear()
    val a = guards.guard(orders)
    val b = guards.guard(orders)
    guards.active.map(_.id) should contain allOf (a.id, b.id)
    a.id should not be b.id
    guards.clear()
    guards.active shouldBe empty
  }

  test("toString is readable in both states") {
    val quiet = guards.guard(orders)
    quiet.toString should include("no failure")

    val crashed = guards.guard(orders)
    runAndCatch(crashed.df.withColumn("boom", explodesOnOutlier(col("amount"))))
    crashed.toString should include("99999")
  }

  test("the accumulator keeps the first report and merges consistently") {
    val a = new CulpritAccumulator
    a.isZero shouldBe true

    val row = org.apache.spark.sql.catalyst.expressions.UnsafeProjection
      .create(Array[org.apache.spark.sql.types.DataType](org.apache.spark.sql.types.IntegerType))
      .apply(org.apache.spark.sql.catalyst.InternalRow(1))
    a.add(CulpritReport(row, 3, 7L, "boom"))
    a.isZero shouldBe false
    a.value.get.partitionId shouldBe 3

    // a second report does not displace the first
    a.add(CulpritReport(row, 9, 1L, "later"))
    a.value.get.partitionId shouldBe 3

    // merging into a populated accumulator keeps what it had
    val b = new CulpritAccumulator
    b.add(CulpritReport(row, 5, 2L, "other"))
    a.merge(b)
    a.value.get.partitionId shouldBe 3

    // merging into an empty one takes the other's
    val c = new CulpritAccumulator
    c.merge(a)
    c.value.get.partitionId shouldBe 3

    a.reset()
    a.isZero shouldBe true
  }
}
