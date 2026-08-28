package org.apache.spark.sql.execution

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, sum, udf}

import org.bigasterisk.api.{BigAsterisk, FuzzConfig, Query, TestGenConfig}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The search-based tools on a pipeline written with the DataFrame API.
 *
 * These tools all work by putting different data under a query and running it again.
 * SQL text makes that free; a DataFrame does not, because its plan is already bound to
 * the data it was built from. What is checked here is that the binding substitutes
 * anyway — and, just as important, that it says so when it cannot, since a search tool
 * that quietly substitutes nothing still reports findings and they are all about the
 * original data.
 */
class RebindSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = BigAsterisk.configure(
      SparkSession.builder()
        .master("local[2]")
        .appName("rebind-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  /** Orders as a DataFrame and nothing else: no view is registered anywhere here. */
  private def orders: DataFrame =
    spark.createDataFrame(Seq(
      ("o1", "c1", 120), ("o2", "c1", 40), ("o3", "c2", 300), ("o4", "c2", 15)))
      .toDF("oid", "cid", "amount")

  private def customers: DataFrame =
    spark.createDataFrame(Seq(("c1", "alice"), ("c2", "bob"))).toDF("cid", "name")

  /** The pipeline under test, built the way anyone would build it. */
  private def pipeline(o: DataFrame, c: DataFrame): DataFrame =
    o.filter(col("amount") > 100)
      .join(c, "cid")
      .groupBy(col("name"))
      .agg(sum(col("amount")).as("total"))

  // -------------------------------------------------------------------------
  // the substitution itself
  // -------------------------------------------------------------------------

  test("a DataFrame pipeline runs against data it was not built from") {
    val (o, c) = (orders, customers)
    val query = Query.Frame(pipeline(o, c))

    val replacement = spark.createDataFrame(Seq(("o9", "c1", 5000)))
      .toDF("oid", "cid", "amount")

    val rerun = BigAsterisk.rerun(spark)
    val rows = rerun.withData(spark, query, Map("orders" -> o), Map("orders" -> replacement))(
      _.collect())

    rows.map(r => (r.getString(0), r.getLong(1))) should contain theSameElementsAs
      Seq(("alice", 5000L))
  }

  test("the original pipeline is unchanged afterwards") {
    val (o, c) = (orders, customers)
    val original = pipeline(o, c)

    BigAsterisk.rerun(spark).withData(
      spark, Query.Frame(original), Map("orders" -> o),
      Map("orders" -> spark.createDataFrame(Seq(("o9", "c1", 5000))).toDF("oid", "cid", "amount"))
    )(_.collect())

    original.collect().map(_.getString(0)).toSet shouldBe Set("alice", "bob")
  }

  test("substituting a DataFrame registers no temporary views") {
    val (o, c) = (orders, customers)
    val before = spark.catalog.listTables().collect().map(_.name).toSet

    BigAsterisk.rerun(spark).withData(
      spark, Query.Frame(pipeline(o, c)), Map("orders" -> o),
      Map("orders" -> orders))(_.collect())

    spark.catalog.listTables().collect().map(_.name).toSet shouldBe before
  }

  test("columns are matched by name, not by position") {
    val (o, c) = (orders, customers)
    // the same columns, declared in a different order
    val shuffled = spark.createDataFrame(Seq((7000, "c2", "o9"))).toDF("amount", "cid", "oid")

    val rows = BigAsterisk.rerun(spark).withData(
      spark, Query.Frame(pipeline(o, c)), Map("orders" -> o),
      Map("orders" -> shuffled))(_.collect())

    rows.map(r => (r.getString(0), r.getLong(1))) should contain theSameElementsAs
      Seq(("bob", 7000L))
  }

  test("a substitute derived from the table it replaces terminates") {
    // Delta debugging works exactly this way: it hands back the base table filtered
    // down to a candidate subset. That substitute still contains the table inside it,
    // so a traversal that re-examined its own output would never finish.
    orders.createOrReplaceTempView("rebind_recursive")
    try {
      val held = spark.table("rebind_recursive")
      val query = Query.Frame(held.filter(col("amount") > 100).select(col("oid")))
      val subset = held.filter(col("oid") === "o3")

      val rows = BigAsterisk.rerun(spark).withData(
        spark, query, Map("rebind_recursive" -> held),
        Map("rebind_recursive" -> subset))(_.collect())

      rows.map(_.getString(0)).toSeq shouldBe Seq("o3")
    } finally spark.catalog.dropTempView("rebind_recursive")
  }

  test("a seed that is itself derived is substituted whole, not inside") {
    orders.createOrReplaceTempView("rebind_derived")
    try {
      // the caller names a *sample* as the table; substituting must replace the sample,
      // not the scan buried under it, or the limit would silently survive
      val sample = spark.table("rebind_derived").limit(2)
      val query = Query.Frame(sample.select(col("oid")))
      val replacement = spark.createDataFrame(
        Seq(("x1", "c1", 1), ("x2", "c1", 2), ("x3", "c1", 3), ("x4", "c1", 4)))
        .toDF("oid", "cid", "amount")

      val rows = BigAsterisk.rerun(spark).withData(
        spark, query, Map("rebind_derived" -> sample),
        Map("rebind_derived" -> replacement))(_.collect())

      rows.map(_.getString(0)).toSet shouldBe Set("x1", "x2", "x3", "x4")
    } finally spark.catalog.dropTempView("rebind_derived")
  }

  test("a table the pipeline does not read is refused, by name") {
    val (o, c) = (orders, customers)
    val query = Query.Frame(pipeline(o, c))

    val thrown = intercept[IllegalArgumentException] {
      BigAsterisk.rerun(spark).requireSubstitutable(
        query, Map("orders" -> o, "shipments" -> customers), "Fuzzing")
    }
    thrown.getMessage should include("shipments")
    thrown.getMessage should include("the DataFrame the pipeline was built from")
  }

  test("a pipeline over temporary views is substitutable under those names") {
    orders.createOrReplaceTempView("rebind_orders")
    try {
      val viewed = spark.table("rebind_orders").filter(col("amount") > 100)
      BigAsterisk.rerun(spark).substitutable(
        Query.Frame(viewed), Map("rebind_orders" -> orders)) shouldBe Set("rebind_orders")
    } finally spark.catalog.dropTempView("rebind_orders")
  }

  // -------------------------------------------------------------------------
  // the tools built on it
  // -------------------------------------------------------------------------

  test("fuzzing a DataFrame pipeline covers the same branches as the SQL form") {
    val (o, c) = (orders, customers)
    o.createOrReplaceTempView("orders")
    c.createOrReplaceTempView("customers")

    val seeds = Map("orders" -> o, "customers" -> c)
    val config = FuzzConfig(iterations = 15, seed = 3)

    val text = BigAsterisk.fuzz(spark).fuzz(
      Query.Sql("SELECT c.name, SUM(o.amount) AS total FROM orders o " +
        "JOIN customers c ON o.cid = c.cid WHERE o.amount > 100 GROUP BY c.name"),
      seeds, config)
    val frame = BigAsterisk.fuzz(spark).fuzz(Query.Frame(pipeline(o, c)), seeds, config)

    // The same branches, reached the same number of times. The labels themselves are
    // not identical: SQL text carries the table alias it was written with (`o.amount`)
    // and the DataFrame form has no alias to carry, so they are compared unqualified.
    def unqualified(covered: Set[String]): Set[String] =
      covered.map(_.replaceAll("[A-Za-z_][A-Za-z0-9_]*\\.(?=[A-Za-z_])", ""))

    unqualified(frame.covered) shouldBe unqualified(text.covered)
    frame.totalBranches shouldBe text.totalBranches
    frame.iterations shouldBe config.iterations
  }

  test("fuzzing a DataFrame pipeline generates inputs that reach both sides of a filter") {
    val (o, c) = (orders, customers)
    val result = BigAsterisk.fuzz(spark).fuzz(
      Query.Frame(pipeline(o, c)),
      Map("orders" -> o, "customers" -> c),
      FuzzConfig(iterations = 25, seed = 11))

    result.covered.size should be > 0

    // The campaign really did feed it something other than the seed rows — which is the
    // whole question for a DataFrame, whose plan would otherwise keep reading its own
    // data however many iterations were asked for. Generated rows carry no schema, so
    // `amount` is read by position.
    val amounts = result.samples.flatMap(_.tables.getOrElse("orders", Seq.empty))
      .map(_.getInt(2)).toSet
    amounts should not be empty
    amounts should not be orders.collect().map(_.getInt(2)).toSet
  }

  test("test generation solves a DataFrame pipeline's own conditions") {
    val (o, c) = (orders, customers)
    val suite = BigAsterisk.testgen(spark).generate(
      Query.Frame(o.filter(col("amount") > 100).select(col("oid"))),
      Map("orders" -> o),
      TestGenConfig(rowsPerPath = 1, natural = false, seed = 5))

    suite.cases.map(_.path) should contain("(amount > 100)")
    suite.verified should not be empty
  }

  test("a Scala UDF's branches are targets in the DataFrame form too") {
    val (o, _) = (orders, customers)
    val band = udf((amount: Int) => if (amount < 50) "small" else "large")

    val suite = BigAsterisk.testgen(spark).generate(
      Query.Frame(o.filter(band(col("amount")) === "large").select(col("oid"))),
      Map("orders" -> o),
      TestGenConfig(rowsPerPath = 1, natural = false, seed = 5))

    // the UDF's own branch is solved as a condition on `amount`
    suite.cases.map(_.path).mkString(" ") should include("amount")
  }
}
