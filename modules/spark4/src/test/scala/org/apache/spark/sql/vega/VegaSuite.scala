package org.apache.spark.sql.vega

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class VegaSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("vega-test")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("src/test/resources/orders_csv").createOrReplaceTempView("orders")
    spark.read.schema("cid STRING, name STRING")
      .csv("src/test/resources/customers_csv").createOrReplaceTempView("customers")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def newEngine(max: Int = VegaEngine.DefaultMaxMaterialized) = new VegaEngine(max)

  private def usesCache(df: org.apache.spark.sql.DataFrame): Boolean =
    df.queryExecution.executedPlan.exists {
      case _: InMemoryTableScanExec => true
      case s: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec =>
        s.inputPlan.exists(_.isInstanceOf[InMemoryTableScanExec])
      case _ => false
    }

  test("a revision reuses the part it shares with the previous query") {
    val vega = newEngine()
    try {
      val first = vega.run(spark.sql("SELECT cid, amount FROM orders WHERE amount > 100"))
      first.df.collect()
      first.reused shouldBe empty
      first.materialized should not be empty

      val second = vega.run(spark.sql(
        "SELECT cid, SUM(amount) AS total FROM orders WHERE amount > 100 GROUP BY cid"))
      second.reused should not be empty
      second.reused.exists(_.contains("Filter")) shouldBe true
      second.reuseRatio should be > 0.0
    } finally vega.clear()
  }

  test("reuse does not change the answer") {
    val vega = newEngine()
    try {
      val sql = "SELECT cid, SUM(amount) AS total FROM orders WHERE amount > 100 GROUP BY cid"
      // the truth, computed with no materialization at all
      val expected = spark.sql(sql).collect().map(r => (r.getString(0), r.getLong(1))).toSet

      vega.run(spark.sql("SELECT cid, amount FROM orders WHERE amount > 100")).df.collect()
      val revised = vega.run(spark.sql(sql))
      revised.reused should not be empty

      val actual = revised.df.collect().map(r => (r.getString(0), r.getLong(1))).toSet
      actual shouldBe expected
    } finally vega.clear()
  }

  test("the revision actually reads from the materialized result") {
    val vega = newEngine()
    try {
      vega.run(spark.sql("SELECT cid, amount FROM orders WHERE amount > 100")).df.collect()
      val revised = vega.run(spark.sql(
        "SELECT cid, SUM(amount) AS total FROM orders WHERE amount > 100 GROUP BY cid"))
      revised.df.collect()
      usesCache(revised.df) shouldBe true
    } finally vega.clear()
  }

  test("an unrelated query reuses nothing") {
    val vega = newEngine()
    try {
      vega.run(spark.sql("SELECT cid, amount FROM orders WHERE amount > 100")).df.collect()
      val unrelated = vega.run(spark.sql("SELECT cid, name FROM customers"))
      unrelated.reused shouldBe empty
    } finally vega.clear()
  }

  test("a changed predicate invalidates the part that contains it") {
    val vega = newEngine()
    try {
      vega.run(spark.sql("SELECT cid, amount FROM orders WHERE amount > 100")).df.collect()
      // the filter differs, so its result cannot be reused
      val changed = vega.run(spark.sql("SELECT cid, amount FROM orders WHERE amount > 200"))
      changed.reused.exists(_.contains("amount > 100")) shouldBe false
    } finally vega.clear()
  }

  test("reuse survives the query being parsed separately") {
    val vega = newEngine()
    try {
      val sql = "SELECT o.oid, o.amount FROM orders o JOIN customers c ON o.cid = c.cid"
      vega.run(spark.sql(sql)).df.collect()
      // same text, parsed again: different attribute ids, same canonicalized plan
      val again = vega.run(spark.sql(sql + " WHERE o.amount > 100"))
      again.reused.exists(_.contains("Join")) shouldBe true
    } finally vega.clear()
  }

  test("materialization stops at the cap") {
    val vega = newEngine(max = 1)
    try {
      vega.run(spark.sql(
        """SELECT c.name, SUM(o.amount) AS total
          |FROM orders o JOIN customers c ON o.cid = c.cid
          |WHERE o.amount > 100 GROUP BY c.name""".stripMargin)).df.collect()
      vega.materialized should have size 1
    } finally vega.clear()
  }

  test("sources and the final result are not materialized") {
    val vega = newEngine()
    try {
      val run = vega.run(spark.sql("SELECT cid, amount FROM orders WHERE amount > 100"))
      run.df.collect()
      // a leaf scan is the storage layer's job; the root is what a revision changes
      run.materialized.exists(_.startsWith("Relation")) shouldBe false
      run.materialized.exists(_.startsWith("Project")) shouldBe false
    } finally vega.clear()
  }

  test("clear releases everything") {
    val vega = newEngine()
    vega.run(spark.sql("SELECT cid, amount FROM orders WHERE amount > 100")).df.collect()
    vega.materialized should not be empty
    vega.clear()
    vega.materialized shouldBe empty
    // and a later revision no longer reports reuse
    vega.run(spark.sql(
      "SELECT cid, SUM(amount) FROM orders WHERE amount > 100 GROUP BY cid")
    ).reused shouldBe empty
    vega.clear()
  }

  test("a negative cap is rejected") {
    an[IllegalArgumentException] should be thrownBy new VegaEngine(-1)
  }

  test("reuseRatio reports the share of parts that were reused") {
    val vega = newEngine()
    try {
      vega.run(spark.sql("SELECT cid, amount FROM orders WHERE amount > 100")).df.collect()
      val revised = vega.run(spark.sql(
        "SELECT cid, SUM(amount) AS total FROM orders WHERE amount > 100 GROUP BY cid"))
      revised.reuseRatio shouldBe (revised.reused.size.toDouble / revised.steps)
      revised.reuseRatio should (be > 0.0 and be <= 1.0)
    } finally vega.clear()
  }
}
