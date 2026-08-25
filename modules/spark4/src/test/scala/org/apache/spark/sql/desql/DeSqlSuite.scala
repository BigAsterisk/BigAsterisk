package org.apache.spark.sql.desql

import org.apache.spark.sql.SparkSession

import org.bigasterisk.api.QueryStep

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DeSqlSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _
  private val engine = new DeSqlEngine

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("desql-test")
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

  private def steps(sql: String): Seq[QueryStep] = engine.decompose(spark.sql(sql))

  test("a filter-aggregate query decomposes into its constituent parts") {
    val s = steps(
      "SELECT cid, SUM(amount) AS total FROM orders WHERE amount > 100 GROUP BY cid")

    s.map(_.operator) should contain allOf ("Filter", "Aggregate")
    // leaves first, final result last
    s.head.childIds shouldBe empty
    s.last.operator shouldBe "Aggregate"
  }

  test("children are listed before the step that consumes them") {
    val s = steps(
      """SELECT c.name, SUM(o.amount) AS total
        |FROM orders o JOIN customers c ON o.cid = c.cid
        |WHERE o.amount > 100 GROUP BY c.name""".stripMargin)

    s.zipWithIndex.foreach { case (step, i) =>
      step.id shouldBe i
      step.childIds.foreach(child => child should be < step.id)
    }
  }

  test("intermediate data is the query up to that step, and no further") {
    val s = steps("SELECT cid, amount FROM orders WHERE amount > 100")

    val scan = s.find(_.childIds.isEmpty).get
    scan.data.count() shouldBe 12          // every order

    val filter = s.find(_.operator == "Filter").get
    filter.data.count() shouldBe 8         // 420,250,310,190,99999,205,380,110
    filter.data.collect().map(_.getInt(2)).foreach(_ should be > 100)
  }

  test("the last step reproduces the original query's answer") {
    val sql = """SELECT c.name, SUM(o.amount) AS total
                |FROM orders o JOIN customers c ON o.cid = c.cid
                |GROUP BY c.name""".stripMargin
    val expected = spark.sql(sql).collect().map(r => (r.getString(0), r.getLong(1))).toSet

    val last = steps(sql).last
    last.data.collect().map(r => (r.getString(0), r.getLong(1))).toSet shouldBe expected
  }

  test("a join step exposes both inputs and the joined rows") {
    val s = steps(
      "SELECT o.oid, c.name FROM orders o JOIN customers c ON o.cid = c.cid")

    val join = s.find(_.operator == "Join").get
    join.childIds should have size 2
    join.detail should include("INNER")
    join.detail should include("o.cid = c.cid")
    // every order matches exactly one customer
    join.data.count() shouldBe 12
  }

  test("wrapper nodes are folded away rather than shown as steps") {
    val s = steps("SELECT o.amount FROM orders o WHERE o.amount > 100")
    val ops = s.map(_.operator)
    ops should not contain "SubqueryAlias"
    ops should not contain "View"
  }

  test("a scan is named after the table it reads, with the query's alias") {
    val aliased = steps("SELECT o.amount FROM orders o WHERE o.amount > 100")
    aliased.find(_.operator == "Relation").get.detail shouldBe "orders AS o"

    // without an alias the name collapses to just the table
    val plain = steps("SELECT amount FROM orders WHERE amount > 100")
    plain.find(_.operator == "Relation").get.detail shouldBe "orders"
  }

  test("relation naming collapses duplicate wrapper names") {
    DeSqlEngine.relationName(Nil) shouldBe None
    DeSqlEngine.relationName(List("orders")) shouldBe Some("orders")
    DeSqlEngine.relationName(List("orders", "orders")) shouldBe Some("orders")
    DeSqlEngine.relationName(List("o", "orders", "orders")) shouldBe Some("orders AS o")
  }

  test("steps follow the analyzed plan, not the optimized one") {
    // WHERE sits above the join in the analyzed plan; the optimizer would push it down
    val s = steps(
      """SELECT o.oid FROM orders o JOIN customers c ON o.cid = c.cid
        |WHERE o.amount > 100""".stripMargin)
    val join = s.find(_.operator == "Join").get
    val filter = s.find(_.operator == "Filter").get
    filter.id should be > join.id
  }

  test("a plan reused by both sides of a self-join becomes a single step") {
    val s = steps(
      "SELECT a.oid, b.oid FROM orders a JOIN orders b ON a.cid = b.cid WHERE a.amount > 400")
    val join = s.find(_.operator == "Join").get
    // the two sides differ (one is filtered), so they are distinct steps
    join.childIds.distinct should have size 2
    // but no step is duplicated
    s.map(_.id) shouldBe s.map(_.id).distinct
  }

  test("detail renders the operator's expressions as SQL text") {
    val s = steps("SELECT cid, SUM(amount) AS total FROM orders WHERE amount > 100 GROUP BY cid")
    s.find(_.operator == "Filter").get.detail should include("amount")
    val agg = s.find(_.operator == "Aggregate").get
    agg.detail should include("GROUP BY")
    agg.detail.toLowerCase should include("sum")
  }

  test("schema is available without materialising the step") {
    val s = steps("SELECT cid, SUM(amount) AS total FROM orders GROUP BY cid")
    s.last.schema.fieldNames shouldBe Array("cid", "total")
  }

  test("a filter contributes its condition and the negation as branches") {
    val filter = steps("SELECT cid FROM orders WHERE amount > 100")
      .find(_.operator == "Filter").get
    val descriptions = filter.branches.map(_.description)
    descriptions.exists(d => d.contains("amount") && d.contains("100")) shouldBe true
    descriptions.exists(_.contains("NOT")) shouldBe true

    // the two arms partition the input
    val taken = filter.branches.find(!_.description.contains("NOT")).get
    val notTaken = filter.branches.find(_.description.contains("NOT")).get
    taken.data.count() shouldBe 8
    notTaken.data.count() shouldBe 4
    taken.data.count() + notTaken.data.count() shouldBe 12
  }

  test("a CASE WHEN contributes each arm's predicate") {
    val s = steps(
      """SELECT cid, SUM(CASE WHEN amount > 1000 THEN -amount ELSE amount END) AS total
        |FROM orders GROUP BY cid""".stripMargin)
    val agg = s.find(_.operator == "Aggregate").get
    val branch = agg.branches.find(_.description.contains("1000"))
    branch shouldBe defined
    // exactly one order exceeds 1000
    branch.get.data.count() shouldBe 1
  }

  test("steps with no conditional expressions have no branches") {
    val s = steps("SELECT cid, amount FROM orders")
    s.filter(_.childIds.nonEmpty).foreach(_.branches shouldBe empty)
  }

  test("a join's condition is not offered as a branch") {
    // the join already discriminates by which rows survive it; scoring the condition
    // separately would double-count
    val join = steps("SELECT o.oid FROM orders o JOIN customers c ON o.cid = c.cid")
      .find(_.operator == "Join").get
    join.branches shouldBe empty
  }

  test("aggregate expressions are not offered as branches") {
    // they cannot be evaluated row by row, so they cannot select input rows
    val agg = steps("SELECT cid, SUM(amount) AS total FROM orders GROUP BY cid")
      .find(_.operator == "Aggregate").get
    agg.branches.map(_.description).exists(_.toLowerCase.contains("sum")) shouldBe false
  }

  test("decompose accepts query text directly") {
    val byText = engine.decompose(spark, "SELECT cid FROM orders WHERE amount > 100")
    val byDf = engine.decompose(spark.sql("SELECT cid FROM orders WHERE amount > 100"))
    byText.map(_.operator) shouldBe byDf.map(_.operator)
  }
}
