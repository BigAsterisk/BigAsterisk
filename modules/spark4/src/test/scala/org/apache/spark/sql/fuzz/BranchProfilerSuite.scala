package org.apache.spark.sql.fuzz

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.desql.DeSqlEngine

import org.bigasterisk.api.BigAsterisk

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The analysis both splicing strategies stand on: which regions of which dataset decide
 * each branch, and how each row decides it.
 */
class BranchProfilerSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = BigAsterisk.configure(
      SparkSession.builder()
        .master("local[2]")
        .appName("branchprofiler-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")
    // deliberately shares the column name `cid` with orders, but is never joined on it
    spark.read.schema("cid STRING, region STRING")
      .csv("src/test/resources/customers_csv").createOrReplaceTempView("regions")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def orders: DataFrame =
    spark.read.schema("oid STRING, cid STRING, amount INT").csv("src/test/resources/orders_csv")
  private def customers: DataFrame =
    spark.read.schema("cid STRING, name STRING").csv("src/test/resources/customers_csv")

  private def schemas = Map("orders" -> orders.schema, "customers" -> customers.schema)

  private def analyzed(sql: String): LogicalPlan = spark.sql(sql).queryExecution.analyzed

  private def branches(plan: LogicalPlan): Seq[(LogicalPlan, Seq[Expression])] =
    DeSqlEngine.stepNodes(plan).flatMap { node =>
      val conditions = DeSqlEngine.branchConditions(node.plan)
      if (conditions.isEmpty) None else Some(node.plan.children.head -> conditions)
    }

  private def influencesOf(sql: String): Seq[BranchProfiler.Influence] = {
    val plan = analyzed(sql)
    val leafTables = LocalDataflow.leafTables(plan, schemas).getOrElse(
      fail("could not match leaves to tables"))
    BranchProfiler.influences(plan, branches(plan), leafTables)
  }

  test("a branch is attributed to exactly the columns that decide it") {
    val influences = influencesOf("SELECT oid FROM orders WHERE amount > 100")
    val positive = influences.find(i => !i.condition.startsWith("NOT")).get

    positive.columns shouldBe Map("orders" -> Set("amount"))
    positive.table shouldBe Some("orders")
    positive.isJoint shouldBe false
    // `oid` and `cid` appear in the query but decide nothing
    positive.columns("orders") should not contain "oid"
  }

  test("a branch over several columns names all of them") {
    val influences = influencesOf(
      "SELECT oid FROM orders WHERE amount > 100 AND cid = 'c1'")
    val positive = influences.find(i => !i.condition.startsWith("NOT")).get
    positive.columns("orders") shouldBe Set("amount", "cid")
  }

  test("a branch decided by two datasets is reported as joint") {
    val influences = influencesOf(
      """SELECT o.oid FROM orders o JOIN customers c ON o.cid = c.cid
        |WHERE o.amount > 100 AND c.name = 'Bob'""".stripMargin)
    val joint = influences.find(_.isJoint)
    joint shouldBe defined
    joint.get.tables shouldBe Set("orders", "customers")
    // a joint branch cannot be evaluated against a single table's row
    joint.get.bound shouldBe empty
    joint.get.table shouldBe empty
  }

  test("join equalities are resolved by expression id, not by column name") {
    val plan = analyzed(
      "SELECT o.oid, c.name FROM orders o JOIN customers c ON o.cid = c.cid")
    val leafTables = LocalDataflow.leafTables(plan, schemas).get
    val constraints = BranchProfiler.joinConstraints(plan, leafTables)

    constraints should have size 1
    constraints.head shouldBe Map("orders" -> Set("cid"), "customers" -> Set("cid"))
  }

  test("columns that merely share a name are not treated as joined") {
    // `regions` also has a `cid`, but the query never joins on it
    val withRegions = Map("orders" -> orders.schema,
      "regions" -> spark.table("regions").schema)
    val plan = analyzed("SELECT o.oid FROM orders o, regions r WHERE o.amount > 100")
    LocalDataflow.leafTables(plan, withRegions).foreach { leafTables =>
      BranchProfiler.joinConstraints(plan, leafTables) shouldBe empty
    }
  }

  test("a path vector records how each row decides each branch") {
    val influences = influencesOf("SELECT oid FROM orders WHERE amount > 100")
    val rows = orders.collect().toSeq
    val vectors = BranchProfiler.pathVectors("orders", rows, orders.schema, influences)

    vectors should have size rows.size
    val positive = influences.indexWhere(i => !i.condition.startsWith("NOT"))

    rows.zip(vectors).foreach { case (row, vector) =>
      vector.bits(positive) shouldBe Some(row.getInt(2) > 100)
    }
    // eight of the twelve orders exceed 100
    vectors.count(_.bits(positive).contains(true)) shouldBe 8
  }

  test("a branch another table decides leaves this table's bit absent") {
    val influences = influencesOf(
      """SELECT o.oid FROM orders o JOIN customers c ON o.cid = c.cid
        |WHERE c.name = 'Bob'""".stripMargin)
    val vectors = BranchProfiler.pathVectors(
      "orders", orders.collect().toSeq, orders.schema, influences)
    // the branch is decided by `customers`, so an order has nothing to say about it
    val decidedByCustomers = influences.indexWhere(_.columns.keySet == Set("customers"))
    if (decidedByCustomers >= 0) {
      vectors.foreach(_.bits(decidedByCustomers) shouldBe None)
    }
  }

  test("minimisation keeps every distinct vector and bounds each one") {
    val influences = influencesOf("SELECT oid FROM orders WHERE amount > 100")
    val rows = orders.collect().toSeq
    val vectors = BranchProfiler.pathVectors("orders", rows, orders.schema, influences)

    val reduced = BranchProfiler.minimise(rows, vectors, perVector = 2)

    // twelve rows fall into two behaviours, so at most two of each survive
    reduced.size should be <= 4
    reduced.map(_._2.toString).toSet shouldBe vectors.map(_.toString).toSet
    reduced.groupBy(_._2.toString).values.foreach(_.size should be <= 2)
  }

  test("minimisation of an empty corpus is empty, and a bad bound is rejected") {
    BranchProfiler.minimise(Seq.empty, Seq.empty, perVector = 1) shouldBe empty
    an[IllegalArgumentException] should be thrownBy
      BranchProfiler.minimise(Seq.empty, Seq.empty, perVector = 0)
  }

  test("a query with no branches profiles to nothing rather than failing") {
    influencesOf("SELECT oid, amount FROM orders") shouldBe empty
    BranchProfiler.pathVectors(
      "orders", orders.collect().toSeq, orders.schema, Seq.empty).foreach {
      _.bits shouldBe empty
    }
  }
}
