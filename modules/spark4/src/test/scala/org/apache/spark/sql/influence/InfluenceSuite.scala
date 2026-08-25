package org.apache.spark.sql.influence

import org.apache.spark.sql.SparkSession

import org.bigasterisk.api.BigAsterisk

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class InfluenceSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = BigAsterisk.configure(
      SparkSession.builder()
        .master("local[2]")
        .appName("influence-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    // c2 holds the outlier: 250, 190, 99999, 205
    spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("src/test/resources/orders_csv").createOrReplaceTempView("orders")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def influence = BigAsterisk.influence(spark)

  test("only the maximum influences a MAX") {
    val df = spark.sql("SELECT cid, MAX(amount) AS peak FROM orders GROUP BY cid")
    val ranked = influence.influencers(df, "peak > 1000")

    // provenance would return all four c2 orders; influence returns the one that decided it
    ranked.head.score shouldBe 1.0 +- 1e-9
    ranked.head.row.getInt(2) shouldBe 99999
    ranked.head.reason should include("maximum")
    ranked.filter(_.score > 0.0) should have size 1
  }

  test("only the minimum influences a MIN") {
    val df = spark.sql("SELECT cid, MIN(amount) AS trough FROM orders GROUP BY cid")
    val ranked = influence.influencers(df, "cid = 'c1'")
    ranked.head.score shouldBe 1.0 +- 1e-9
    ranked.head.row.getInt(2) shouldBe 60      // c1's smallest
    ranked.head.reason should include("minimum")
  }

  test("influence on a SUM is the size of the contribution") {
    val df = spark.sql("SELECT cid, SUM(amount) AS total FROM orders GROUP BY cid")
    val ranked = influence.influencers(df, "total > 50000")

    ranked.head.row.getInt(2) shouldBe 99999
    // 99999 of (250 + 190 + 99999 + 205)
    ranked.head.score should be > 0.99
    ranked.head.reason should include("contribution")
    // shares of one group sum to 1
    ranked.map(_.score).sum shouldBe 1.0 +- 1e-6
  }

  test("every record counts equally toward a COUNT") {
    val df = spark.sql("SELECT cid, COUNT(*) AS n FROM orders GROUP BY cid")
    val ranked = influence.influencers(df, "cid = 'c3'")
    ranked should have size 4
    ranked.map(_.score).distinct should have size 1
    ranked.head.reason should include("equally")
    ranked.map(_.score).sum shouldBe 1.0 +- 1e-9
  }

  test("a global aggregate is one group over every record") {
    val df = spark.sql("SELECT MAX(amount) AS peak FROM orders")
    val ranked = influence.influencers(df, "peak > 1000")
    ranked.head.row.getInt(2) shouldBe 99999
    ranked.head.score shouldBe 1.0 +- 1e-9
  }

  test("returned records are the rows that entered the aggregation") {
    val df = spark.sql("SELECT cid, MAX(amount) AS peak FROM orders GROUP BY cid")
    val ranked = influence.influencers(df, "peak > 1000")
    // no bookkeeping columns leak into the answer
    ranked.head.row.schema.fieldNames.toSeq shouldBe Seq("oid", "cid", "amount")
  }

  test("results are ordered most influential first and bounded by topK") {
    val df = spark.sql("SELECT cid, SUM(amount) AS total FROM orders GROUP BY cid")
    val ranked = influence.influencers(df, "total > 50000", topK = 2)
    ranked should have size 2
    ranked.map(_.score) shouldBe ranked.map(_.score).sorted.reverse
  }

  test("a query with no aggregation says so, since provenance is already exact") {
    val df = spark.sql("SELECT oid, amount FROM orders WHERE amount > 1000")
    val ranked = influence.influencers(df, "amount > 1000")
    ranked should not be empty
    ranked.head.reason should include("already exact")
  }

  test("a predicate matching no result is an error naming the available columns") {
    val df = spark.sql("SELECT cid, MAX(amount) AS peak FROM orders GROUP BY cid")
    val e = the[IllegalArgumentException] thrownBy
      influence.influencers(df, "peak > 100000000")
    e.getMessage should include("peak")
  }

  test("a negative topK is rejected") {
    val df = spark.sql("SELECT cid, MAX(amount) AS peak FROM orders GROUP BY cid")
    an[IllegalArgumentException] should be thrownBy
      influence.influencers(df, "peak > 1000", topK = -1)
  }

  test("influence rules behave at the boundaries") {
    import org.apache.spark.sql.catalyst.expressions.aggregate._
    import org.apache.spark.sql.catalyst.expressions.Literal
    val rows = Seq.fill(3)(org.apache.spark.sql.Row(1))

    // all contributions zero: weighted equally rather than divided by zero
    val zeroed = InfluenceEngine.rule(
      Sum(Literal(0)), rows, Seq(Some(0.0), Some(0.0), Some(0.0)))
    zeroed.get.map(_._1).sum shouldBe 1.0 +- 1e-9

    // ties on a maximum share the influence
    val tied = InfluenceEngine.rule(
      Max(Literal(0)), rows, Seq(Some(5.0), Some(5.0), Some(1.0)))
    tied.get.map(_._1) shouldBe Seq(0.5, 0.5, 0.0)

    // nulls contribute nothing to a sum
    val withNull = InfluenceEngine.rule(
      Sum(Literal(0)), rows, Seq(Some(4.0), None, Some(4.0)))
    withNull.get.map(_._1) shouldBe Seq(0.5, 0.0, 0.5)
  }
}
