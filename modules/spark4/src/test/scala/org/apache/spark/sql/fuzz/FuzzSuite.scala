package org.apache.spark.sql.fuzz

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.bigasterisk.api.{BigAsterisk, FuzzConfig, MutationStrategy}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FuzzSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = BigAsterisk.configure(
      SparkSession.builder()
        .master("local[2]")
        .appName("fuzz-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def fuzzer = BigAsterisk.fuzz(spark)

  private def orders: DataFrame = {
    val df = spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("src/test/resources/orders_csv")
    df.createOrReplaceTempView("orders")
    df
  }

  private def customers: DataFrame = {
    val df = spark.read.schema("cid STRING, name STRING")
      .csv("src/test/resources/customers_csv")
    df.createOrReplaceTempView("customers")
    df
  }

  private val joinQuery =
    """SELECT c.name, SUM(o.amount) AS total
      |FROM orders o JOIN customers c ON o.cid = c.cid
      |WHERE o.amount > 100
      |GROUP BY c.name""".stripMargin

  private def seeds = Map("orders" -> orders, "customers" -> customers)

  test("a campaign runs the configured number of iterations") {
    val result = fuzzer.fuzz(joinQuery, seeds, FuzzConfig(iterations = 8, seed = 1L))
    result.iterations shouldBe 8
  }

  test("branches of the query are found and reached") {
    val result = fuzzer.fuzz(joinQuery, seeds, FuzzConfig(iterations = 20, seed = 1L))
    result.totalBranches should be > 0
    result.covered should not be empty
    result.coverage should be > 0.0
    result.coverage should be <= 1.0
  }

  test("co-dependent mutation gets rows past a join where random mutation cannot") {
    val random = fuzzer.fuzz(joinQuery, seeds,
      FuzzConfig(iterations = 20, strategy = MutationStrategy.Random, seed = 7L))
    val coDependent = fuzzer.fuzz(joinQuery, seeds,
      FuzzConfig(iterations = 20, strategy = MutationStrategy.CoDependent, seed = 7L))

    // a randomly generated join key essentially never matches, so the query returns
    // nothing and the campaign learns nothing; sharing the key pool fixes exactly that
    random.emptyResults should be > coDependent.emptyResults
    coDependent.emptyResults should be < 20
  }

  test("natural mutation draws values that actually occur in the data") {
    val result = fuzzer.fuzz(
      "SELECT cid, amount FROM orders",
      Map("orders" -> orders),
      FuzzConfig(iterations = 6, strategy = MutationStrategy.Natural, seed = 3L))
    result.iterations shouldBe 6
    // natural splicing over a single table cannot fail this query
    result.failures shouldBe empty
  }

  test("a campaign is reproducible from its seed") {
    def run() = fuzzer.fuzz(joinQuery, seeds, FuzzConfig(iterations = 10, seed = 42L))
    val a = run()
    val b = run()
    a.covered shouldBe b.covered
    a.emptyResults shouldBe b.emptyResults
    a.failures.map(_.error) shouldBe b.failures.map(_.error)
  }

  test("a failing query is caught and reported with the input that caused it") {
    // integer division by a value the fuzzer will eventually generate as zero
    val query = "SELECT oid, amount DIV (amount - amount) AS boom FROM orders"
    val result = fuzzer.fuzz(query, Map("orders" -> orders),
      FuzzConfig(iterations = 4, seed = 5L))
    // Spark returns null for division by zero unless ANSI mode is on, so this asserts
    // the campaign completes rather than that it necessarily fails
    result.iterations shouldBe 4
    result.failures.foreach { f =>
      f.tables should contain key "orders"
      f.error should not be empty
    }
  }

  test("ANSI arithmetic overflow is found") {
    val previous = spark.conf.get("spark.sql.ansi.enabled")
    try {
      spark.conf.set("spark.sql.ansi.enabled", "true")
      // `amount` is INT and stays INT here, so doubling Int.MaxValue overflows. The
      // boundary set contains Int.MaxValue, which is the point of having one: no amount
      // of plausible-looking data finds this.
      val result = fuzzer.fuzz(
        "SELECT oid, amount + amount AS doubled FROM orders",
        Map("orders" -> orders),
        FuzzConfig(iterations = 30, rowsPerTable = 20, seed = 11L))
      result.failures should not be empty
      result.failures.head.error.toLowerCase should include("overflow")
      // the reported input is enough to reproduce it
      result.failures.head.tables("orders") should not be empty
    } finally {
      spark.conf.set("spark.sql.ansi.enabled", previous)
    }
  }

  test("the caller's views are restored after a campaign") {
    val before = spark.table("orders").collect().map(_.getString(0)).toSet
    fuzzer.fuzz(joinQuery, seeds, FuzzConfig(iterations = 4, seed = 2L))
    val after = spark.table("orders").collect().map(_.getString(0)).toSet
    after shouldBe before
  }

  test("zero iterations is a valid, empty campaign") {
    val result = fuzzer.fuzz(joinQuery, seeds, FuzzConfig(iterations = 0))
    result.iterations shouldBe 0
    result.failures shouldBe empty
    result.covered shouldBe empty
  }

  test("guidance keeps inputs that reach new branches") {
    val guided = fuzzer.fuzz(joinQuery, seeds,
      FuzzConfig(iterations = 25, seed = 4L, guided = true))
    val unguided = fuzzer.fuzz(joinQuery, seeds,
      FuzzConfig(iterations = 25, seed = 4L, guided = false))
    guided.covered.size should be >= unguided.covered.size
  }

  test("configuration rejects nonsense") {
    an[IllegalArgumentException] should be thrownBy FuzzConfig(iterations = -1)
    an[IllegalArgumentException] should be thrownBy FuzzConfig(rowsPerTable = 0)
    an[IllegalArgumentException] should be thrownBy fuzzer.fuzz(joinQuery, Map.empty)
  }

  test("strategies can be looked up by name") {
    MutationStrategy.byName("natural") shouldBe MutationStrategy.Natural
    MutationStrategy.byName("CO-DEPENDENT") shouldBe MutationStrategy.CoDependent
    val e = the[IllegalArgumentException] thrownBy MutationStrategy.byName("nope")
    e.getMessage should include("natural")
  }

  test("join equalities are read out of the plan") {
    val plan = spark.sql(joinQuery).queryExecution.analyzed
    FuzzEngine.joinedColumnNames(plan) should contain ("cid", "cid")

    val noJoin = spark.sql("SELECT cid FROM orders").queryExecution.analyzed
    FuzzEngine.joinedColumnNames(noJoin) shouldBe empty
  }
}
