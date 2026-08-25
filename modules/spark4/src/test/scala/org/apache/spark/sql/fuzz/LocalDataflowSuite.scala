package org.apache.spark.sql.fuzz

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.{Dataset => ClassicDataset, SparkSession => ClassicSparkSession}

import org.bigasterisk.api.BigAsterisk

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Differential tests: interpreting a query must produce exactly what executing it does.
 *
 * This is the only bar that matters for a faster oracle. A campaign that runs a hundred
 * thousand iterations against an interpreter which quietly disagrees with Spark is worse
 * than no campaign at all, so every supported shape is checked against the real thing,
 * and every unsupported one is checked to *say* it is unsupported rather than guess.
 */
class LocalDataflowSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = BigAsterisk.configure(
      SparkSession.builder()
        .master("local[2]")
        .appName("localdataflow-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("src/test/resources/orders_csv").createOrReplaceTempView("orders")
    spark.read.schema("cid STRING, name STRING")
      .csv("src/test/resources/customers_csv").createOrReplaceTempView("customers")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def classic = spark.asInstanceOf[ClassicSparkSession]

  /** The rows of each leaf, read the way the fuzzer supplies generated ones. */
  private def tablesFor(plan: LogicalPlan): Map[String, Seq[org.apache.spark.sql.Row]] =
    LocalDataflow.leaves(plan).map { leaf =>
      LocalDataflow.leafKey(leaf) -> ClassicDataset.ofRows(classic, leaf).collect().toSeq
    }.toMap

  private def interpret(sql: String): LocalDataflow.Outcome = {
    val df = spark.sql(sql)
    val plan = df.queryExecution.analyzed
    LocalDataflow.evaluate(plan, tablesFor(plan))
  }

  /** Interpreting and executing must agree, as multisets of values. */
  private def agrees(sql: String): org.scalatest.Assertion = {
    val expected = spark.sql(sql).collect().map(_.toSeq).toSeq
    interpret(sql) match {
      case LocalDataflow.Rows(rows) =>
        rows.map(_.toSeq).sortBy(_.mkString("")) shouldBe
          expected.sortBy(_.mkString(""))
      case LocalDataflow.Unsupported(reason) => fail(s"expected support, got: $reason")
      case LocalDataflow.Failed(e)           => fail(s"interpreter threw: $e", e)
    }
  }

  test("a bare scan agrees") {
    agrees("SELECT * FROM orders")
  }

  test("projection agrees, including computed columns") {
    agrees("SELECT cid, amount FROM orders")
    agrees("SELECT oid, amount * 2 AS doubled, UPPER(cid) AS upper FROM orders")
  }

  test("filters agree, including compound conditions and nulls") {
    agrees("SELECT * FROM orders WHERE amount > 100")
    agrees("SELECT * FROM orders WHERE amount > 100 AND cid = 'c2'")
    agrees("SELECT * FROM orders WHERE amount > 100 OR cid = 'c3'")
    agrees("SELECT * FROM orders WHERE amount IS NOT NULL")
  }

  test("CASE WHEN agrees") {
    agrees(
      "SELECT cid, CASE WHEN amount > 1000 THEN -amount ELSE amount END AS adjusted FROM orders")
  }

  test("an inner join agrees") {
    agrees("SELECT o.oid, c.name FROM orders o JOIN customers c ON o.cid = c.cid")
    agrees(
      """SELECT o.oid, c.name FROM orders o JOIN customers c ON o.cid = c.cid
        |WHERE o.amount > 100""".stripMargin)
  }

  test("grouped aggregation agrees for every supported function") {
    agrees("SELECT cid, SUM(amount) AS total FROM orders GROUP BY cid")
    agrees("SELECT cid, COUNT(*) AS n FROM orders GROUP BY cid")
    agrees("SELECT cid, MAX(amount) AS peak FROM orders GROUP BY cid")
    agrees("SELECT cid, MIN(amount) AS trough FROM orders GROUP BY cid")
    agrees("SELECT cid, AVG(amount) AS mean FROM orders GROUP BY cid")
    agrees("SELECT cid, SUM(amount) AS total, COUNT(*) AS n FROM orders GROUP BY cid")
  }

  test("a global aggregate agrees") {
    agrees("SELECT SUM(amount) AS total, COUNT(*) AS n FROM orders")
  }

  test("aggregation over a join agrees") {
    agrees(
      """SELECT c.name, SUM(o.amount) AS total
        |FROM orders o JOIN customers c ON o.cid = c.cid
        |WHERE o.amount > 100 GROUP BY c.name""".stripMargin)
  }

  test("union all agrees") {
    agrees("SELECT cid FROM orders UNION ALL SELECT cid FROM customers")
  }

  test("distinct agrees") {
    agrees("SELECT DISTINCT cid FROM orders")
  }

  test("limit agrees in count") {
    interpret("SELECT * FROM orders LIMIT 5") match {
      case LocalDataflow.Rows(rows) => rows.size shouldBe 5
      case other                    => fail(s"expected rows, got $other")
    }
  }

  test("a query that throws under Spark also throws here") {
    val previous = spark.conf.get("spark.sql.ansi.enabled")
    try {
      spark.conf.set("spark.sql.ansi.enabled", "true")
      // dividing by zero is an error under ANSI, and the interpreter must not paper it over
      interpret("SELECT 100 DIV (amount - amount) AS boom FROM orders") match {
        case LocalDataflow.Failed(_) => succeed
        case other                   => fail(s"expected a failure, got $other")
      }
    } finally spark.conf.set("spark.sql.ansi.enabled", previous)
  }

  test("an outer join is refused rather than approximated") {
    interpret(
      "SELECT c.name, o.oid FROM customers c LEFT OUTER JOIN orders o ON o.cid = c.cid") match {
      case LocalDataflow.Unsupported(reason) => reason.toLowerCase should include("join")
      case other                             => fail(s"expected Unsupported, got $other")
    }
  }

  test("ordering is refused rather than approximated") {
    interpret("SELECT * FROM orders ORDER BY amount") match {
      case LocalDataflow.Unsupported(_) => succeed
      case other                        => fail(s"expected Unsupported, got $other")
    }
  }

  test("a distinct aggregate is refused") {
    interpret("SELECT cid, COUNT(DISTINCT amount) AS n FROM orders GROUP BY cid") match {
      case LocalDataflow.Unsupported(reason) => reason.toLowerCase should include("distinct")
      case other                             => fail(s"expected Unsupported, got $other")
    }
  }

  test("missing data for a leaf is refused, not silently empty") {
    val plan = spark.sql("SELECT * FROM orders").queryExecution.analyzed
    LocalDataflow.evaluate(plan, Map.empty) match {
      case LocalDataflow.Unsupported(reason) => reason should include("no data supplied")
      case other                             => fail(s"expected Unsupported, got $other")
    }
  }

  test("substituting different data changes the answer, as the fuzzer needs") {
    import org.apache.spark.sql.Row
    val plan = spark.sql("SELECT cid, SUM(amount) AS total FROM orders GROUP BY cid")
      .queryExecution.analyzed
    val leaf = LocalDataflow.leaves(plan).head
    val substituted = Map(LocalDataflow.leafKey(leaf) -> Seq(
      Row("x1", "z9", 5), Row("x2", "z9", 7)))

    LocalDataflow.evaluate(plan, substituted) match {
      case LocalDataflow.Rows(rows) =>
        rows.map(r => (r.getString(0), r.getLong(1))) shouldBe Seq(("z9", 12L))
      case other => fail(s"expected rows, got $other")
    }
  }
}
