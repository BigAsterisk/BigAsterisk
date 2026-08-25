package org.bigasterisk.optdebug

import org.apache.spark.sql.{Row, SparkSession}

import org.bigasterisk.api.{BigAsterisk, Suspiciousness}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class OptDebugSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = BigAsterisk.configure(
      SparkSession.builder()
        .master("local[2]")
        .appName("optdebug-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    // amounts: c1 -> 420 310 60 110, c2 -> 250 190 99999 205, c3 -> 80 95 75 380
    // The sample data lives with the engine module rather than being copied per tool;
    // forked tests run from this module's directory, hence the relative hop.
    spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("../spark4/src/test/resources/orders_csv").createOrReplaceTempView("orders")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  /**
   * A query with a planted fault: amounts over 1000 are negated, which only the one
   * outlier record (99999, in group c2) ever hits. c2's total comes out negative.
   */
  private val faultyQuery =
    """SELECT cid, SUM(CASE WHEN amount > 1000 THEN -amount ELSE amount END) AS total
      |FROM orders GROUP BY cid""".stripMargin

  private val negativeTotal: Row => Boolean = r => r.getLong(1) < 0

  test("the faulty branch is ranked first") {
    val result = OptDebug.localize(spark, spark.sql(faultyQuery), negativeTotal)

    result.prime shouldBe defined
    val top = result.prime.get
    top.isBranch shouldBe true
    top.branch.get should include("amount")
    top.branch.get should include("1000")
    top.score shouldBe 1.0 +- 1e-9
  }

  test("the faulty branch covers the failing witnesses and no passing ones") {
    val result = OptDebug.localize(spark, spark.sql(faultyQuery), negativeTotal)
    val top = result.prime.get
    top.failingWitnesses should be > 0L
    top.passingWitnesses shouldBe 0L
  }

  test("an operation that touches every record scores neutrally, not first") {
    val result = OptDebug.localize(spark, spark.sql(faultyQuery), negativeTotal)
    val aggregate = result.ranked.find(op => op.operator == "Aggregate" && !op.isBranch)
    aggregate shouldBe defined
    // reaches every witness of both populations: no information, so 0.5
    aggregate.get.score shouldBe 0.5 +- 1e-9
    aggregate.get.score should be < result.prime.get.score
  }

  test("the failing outputs are the ones the oracle rejected") {
    val result = OptDebug.localize(spark, spark.sql(faultyQuery), negativeTotal)
    result.failingOutputs.map(_.getString(0)).toSet shouldBe Set("c2")
    result.failingOutputs.forall(negativeTotal) shouldBe true
  }

  test("both witness populations are reported") {
    val result = OptDebug.localize(spark, spark.sql(faultyQuery), negativeTotal)
    result.failingWitnesses shouldBe 4L   // the four c2 orders
    result.passingWitnesses shouldBe 8L   // c1 and c3
  }

  test("Ochiai is available and ranks differently") {
    val tarantula = OptDebug.localize(spark, spark.sql(faultyQuery), negativeTotal)
    val ochiai = OptDebug.localize(
      spark, spark.sql(faultyQuery), negativeTotal, Suspiciousness.Ochiai)

    ochiai.formula shouldBe "ochiai"
    tarantula.formula shouldBe "tarantula"
    // Ochiai rewards raw failing coverage, so the all-touching aggregate out-ranks the
    // branch — the reason Tarantula is the default
    ochiai.prime.get.isBranch shouldBe false
  }

  test("a filter's condition and its negation are both scored") {
    val result = OptDebug.localize(
      spark,
      spark.sql("SELECT cid, SUM(amount) AS total FROM orders WHERE amount > 50 GROUP BY cid"),
      (r: Row) => r.getString(0) == "c2")
    val filterBranches = result.ranked.filter(op => op.operator == "Filter" && op.isBranch)
    filterBranches.size should be >= 2
    filterBranches.exists(_.branch.get.contains("NOT")) shouldBe true
  }

  test("the ranking is deterministic across runs") {
    def rank() = OptDebug.localize(spark, spark.sql(faultyQuery), negativeTotal)
      .ranked.map(op => (op.stepId, op.branch, op.score))
    rank() shouldBe rank()
  }

  test("source scans are not scored") {
    val result = OptDebug.localize(spark, spark.sql(faultyQuery), negativeTotal)
    result.ranked.exists(_.operator == "Relation") shouldBe false
  }

  test("an oracle that rejects nothing is an error, not an empty ranking") {
    val e = the[IllegalArgumentException] thrownBy
      OptDebug.localize(spark, spark.sql(faultyQuery), _ => false)
    e.getMessage should include("no fault to localise")
  }

  test("the SQL-predicate form agrees with the function form") {
    val byFunction = OptDebug.localize(spark, spark.sql(faultyQuery), negativeTotal)
    val bySql = OptDebug.localize(spark, spark.sql(faultyQuery), "total < 0")

    bySql.prime.get.branch shouldBe byFunction.prime.get.branch
    bySql.prime.get.score shouldBe byFunction.prime.get.score +- 1e-9
    bySql.failingWitnesses shouldBe byFunction.failingWitnesses
    bySql.passingWitnesses shouldBe byFunction.passingWitnesses
  }

  test("the SQL-predicate form hides its bookkeeping column from the reported outputs") {
    val result = OptDebug.localize(spark, spark.sql(faultyQuery), "total < 0")
    result.failingOutputs.foreach { row =>
      row.schema.fieldNames.toSeq shouldBe Seq("cid", "total")
    }
    result.failingOutputs.map(_.getString(0)).toSet shouldBe Set("c2")
  }

  test("formulas can be looked up by name") {
    OptDebug.formulaByName("tarantula") shouldBe Suspiciousness.Tarantula
    OptDebug.formulaByName("OCHIAI") shouldBe Suspiciousness.Ochiai
    val e = the[IllegalArgumentException] thrownBy OptDebug.formulaByName("nope")
    e.getMessage should include("tarantula")
  }

  test("branchOrNull is usable from a language binding") {
    val result = OptDebug.localize(spark, spark.sql(faultyQuery), negativeTotal)
    result.prime.get.branchOrNull should not be null
    result.ranked.find(!_.isBranch).get.branchOrNull shouldBe null
  }

  test("scoring formulas behave at the boundaries") {
    // an operation covering only failing records
    Suspiciousness.Tarantula.score(3, 0, 3, 10) shouldBe 1.0 +- 1e-9
    // one covering only passing records
    Suspiciousness.Tarantula.score(0, 5, 3, 10) shouldBe 0.0 +- 1e-9
    // one covering everything
    Suspiciousness.Tarantula.score(3, 10, 3, 10) shouldBe 0.5 +- 1e-9
    // no observations at all
    Suspiciousness.Tarantula.score(0, 0, 0, 0) shouldBe 0.0
    Suspiciousness.Ochiai.score(0, 0, 0, 0) shouldBe 0.0
    Suspiciousness.Ochiai.score(3, 0, 3, 10) shouldBe 1.0 +- 1e-9
  }
}
