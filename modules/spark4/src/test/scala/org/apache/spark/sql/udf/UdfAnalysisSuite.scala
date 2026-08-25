package org.apache.spark.sql.udf

import java.util.{ArrayList => JArrayList, HashMap => JHashMap}

import org.apache.spark.api.python.SimplePythonFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, Literal, PythonUDF}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.classic.{Dataset => ClassicDataset, SparkSession => ClassicSparkSession}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}

import org.bigasterisk.api.{BigAsterisk, UdfProfile, UdfRegistry}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Binding a Python UDF's internals back to the query that calls it.
 *
 * The profiles here are written by hand, the way the Python analyser would emit them,
 * so that what is under test is the binding — not the parsing of Python.
 */
class UdfAnalysisSuite extends AnyFunSuite with Matchers
    with BeforeAndAfterAll with BeforeAndAfterEach {

  @transient private var spark: ClassicSparkSession = _

  override def beforeAll(): Unit = {
    spark = BigAsterisk.configure(
      SparkSession.builder()
        .master("local[2]")
        .appName("udf-analysis-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")).getOrCreate()
      .asInstanceOf[ClassicSparkSession]
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  override def afterEach(): Unit = UdfRegistry.clear()

  private def orders: DataFrame =
    spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("src/test/resources/orders_csv")

  /** A `PythonUDF` node as the analyzer would leave one in a plan. */
  private def pythonUdf(
      name: String,
      arguments: Seq[Expression],
      dataType: org.apache.spark.sql.types.DataType = StringType,
      evalType: Int = 100): PythonUDF =
    PythonUDF(
      name,
      SimplePythonFunction(
        Seq.empty[Byte], new JHashMap[String, String](), new JArrayList[String](),
        "python3", "3.11", new JArrayList(), null),
      dataType,
      arguments,
      evalType,
      udfDeterministic = true)

  private val classify = UdfProfile.parse(Seq(
    "udf\tclassify\tamount",
    "branch\tamount > 1000\tamount",
    "branch\tamount > 100\tamount",
    "path\tamount > 1000\t'high'\texact",
    "path\t(NOT amount > 1000) AND amount > 100\t'medium'\texact",
    "path\t(NOT amount > 1000) AND (NOT amount > 100)\t'low'\texact",
    "influence\tamount"))

  /** `WHERE classify(amount) = <value>` over the orders table. */
  private def filterOnClassify(value: String): (LogicalPlan, LogicalPlan) = {
    val child = orders.queryExecution.analyzed
    val amount = child.output.find(_.name == "amount").get
    (Filter(EqualTo(pythonUdf("classify", Seq(amount)), Literal(value)), child), child)
  }

  private def sql(e: Expression): String = e.sql

  test("nothing happens without a profile") {
    val (plan, child) = filterOnClassify("high")

    UdfAnalysis.profiled(plan) shouldBe empty
    UdfAnalysis.internalBranches(plan, spark) shouldBe empty
    UdfAnalysis.solveThrough(plan.asInstanceOf[Filter].condition, child, spark) shouldBe empty
  }

  test("a branch inside the function becomes a condition on the column passed to it") {
    UdfRegistry.register(classify)
    val (plan, child) = filterOnClassify("high")

    val branches = UdfAnalysis.internalBranches(plan, spark)
    branches.map(sql) should contain allOf ("(amount > 1000)", "(amount > 100)")

    // and it is a usable predicate: eight of the twelve orders exceed 100
    val above = branches.find(sql(_) == "(amount > 100)").get
    ClassicDataset.ofRows(spark, Filter(above, child)).count() shouldBe 8
  }

  test("testing the function's result becomes a condition on its argument") {
    UdfRegistry.register(classify)
    val (plan, child) = filterOnClassify("high")

    val solved = UdfAnalysis.solveThrough(plan.asInstanceOf[Filter].condition, child, spark)
    solved.map(_.map(sql)) shouldBe Some(Seq("(amount > 1000)"))
  }

  test("a result produced by an inner path carries the outer branch's negation") {
    UdfRegistry.register(classify)
    val (plan, child) = filterOnClassify("medium")

    val solved = UdfAnalysis.solveThrough(plan.asInstanceOf[Filter].condition, child, spark).get
    solved should have size 1
    sql(solved.head) shouldBe "((NOT (amount > 1000)) AND (amount > 100))"

    // the two orders between 100 and 1000 exclusive... every order except the outlier
    // and those at or below 100
    val matching = ClassicDataset.ofRows(spark, Filter(solved.head, child)).collect()
    matching.map(_.getInt(2)).foreach { amount =>
      amount should be > 100
      amount should be <= 1000
    }
  }

  test("a value the function never returns yields nothing to solve") {
    UdfRegistry.register(classify)
    val (plan, child) = filterOnClassify("nonexistent")

    UdfAnalysis.solveThrough(plan.asInstanceOf[Filter].condition, child, spark) shouldBe empty
  }

  test("a profile that could not be fully read is never solved through") {
    UdfRegistry.register(UdfProfile.parse(Seq(
      "udf\tclassify\tamount",
      "path\tamount > 1000\t'high'\tapproximate",
      "unsupported\tmethod 'encode' is not understood")))
    val (plan, child) = filterOnClassify("high")

    // an approximate path may be reached under conditions it does not describe, so
    // standing it in for the call would generate tests that prove nothing
    UdfAnalysis.solveThrough(plan.asInstanceOf[Filter].condition, child, spark) shouldBe empty
  }

  test("a boolean UDF used as a predicate on its own is solved for true") {
    UdfRegistry.register(UdfProfile.parse(Seq(
      "udf\tis_big\tamount",
      "branch\tamount > 500\tamount",
      "path\tamount > 500\tTRUE\texact",
      "path\t(NOT amount > 500)\tFALSE\texact",
      "influence\tamount")))

    val child = orders.queryExecution.analyzed
    val amount = child.output.find(_.name == "amount").get
    val condition = pythonUdf("is_big", Seq(amount), BooleanType)

    UdfAnalysis.solveThrough(condition, child, spark).map(_.map(sql)) shouldBe
      Some(Seq("(amount > 500)"))
  }

  test("a profile is not applied to a call with a different number of arguments") {
    UdfRegistry.register(classify)
    val child = orders.queryExecution.analyzed
    val amount = child.output.find(_.name == "amount").get
    val cid = child.output.find(_.name == "cid").get
    // same name, two arguments: this cannot be the function that was profiled
    val plan = Filter(EqualTo(pythonUdf("classify", Seq(amount, cid)), Literal("high")), child)

    UdfAnalysis.profiled(plan) shouldBe empty
    UdfAnalysis.internalBranches(plan, spark) shouldBe empty
  }

  test("a pandas UDF is left alone: its parameters are Series, not values") {
    UdfRegistry.register(classify)
    val child = orders.queryExecution.analyzed
    val amount = child.output.find(_.name == "amount").get
    // SQL_SCALAR_PANDAS_UDF
    val plan = Filter(EqualTo(pythonUdf("classify", Seq(amount), StringType, 200),
      Literal("high")), child)

    UdfAnalysis.profiled(plan) shouldBe empty
    UdfAnalysis.internalBranches(plan, spark) shouldBe empty
  }

  test("a condition naming something that is not a parameter is dropped") {
    UdfRegistry.register(UdfProfile.parse(Seq(
      "udf\tclassify\tamount",
      "branch\tmystery > 1000\tmystery",
      "path\ttrue\t'high'\texact")))
    val (plan, _) = filterOnClassify("high")

    // binding it would silently pick up a column of the same name from the query
    UdfAnalysis.internalBranches(plan, spark) shouldBe empty
  }

  test("a condition that is not a boolean is dropped") {
    UdfRegistry.register(UdfProfile.parse(Seq(
      "udf\tclassify\tamount",
      "branch\tamount + 1\tamount",
      "path\ttrue\t'high'\texact")))
    val (plan, _) = filterOnClassify("high")

    UdfAnalysis.internalBranches(plan, spark) shouldBe empty
  }

  test("influence stops at an argument the function cannot return") {
    // `def score(amount, note): return amount * 2` — note is passed and ignored
    UdfRegistry.register(UdfProfile.parse(Seq(
      "udf\tscore\tamount,note", "path\ttrue\t\texact", "influence\tamount")))

    val child = orders.queryExecution.analyzed
    val amount = child.output.find(_.name == "amount").get
    val cid = child.output.find(_.name == "cid").get
    val call = pythonUdf("score", Seq(amount, cid), IntegerType)

    call.references.map(_.name).toSet shouldBe Set("amount", "cid")
    UdfAnalysis.influencing(call).map(_.name) shouldBe Set("amount")
  }

  test("influence over an unprofiled function stays at every argument it reads") {
    val child = orders.queryExecution.analyzed
    val amount = child.output.find(_.name == "amount").get
    val cid = child.output.find(_.name == "cid").get
    val call = pythonUdf("score", Seq(amount, cid), IntegerType)

    // no profile: nothing is known, so nothing is narrowed
    UdfAnalysis.influencing(call).map(_.name) shouldBe Set("amount", "cid")
  }

  test("influence through an expression wrapping a call keeps the rest of it") {
    UdfRegistry.register(UdfProfile.parse(Seq(
      "udf\tscore\tamount,note", "path\ttrue\t\texact", "influence\tamount")))

    val child = orders.queryExecution.analyzed
    val Seq(oid, cid, amount) = child.output.take(3)
    val call = pythonUdf("score", Seq(amount, cid), IntegerType)
    val wrapper = org.apache.spark.sql.catalyst.expressions.Coalesce(Seq(call, oid))

    UdfAnalysis.influencing(wrapper).map(_.name) shouldBe Set("amount", "oid")
  }

  test("a UDF in a projection is profiled too, not only one in a filter") {
    UdfRegistry.register(classify)
    val child = orders.queryExecution.analyzed
    val amount = child.output.find(_.name == "amount").get
    val alias = org.apache.spark.sql.catalyst.expressions.Alias(
      pythonUdf("classify", Seq(amount)), "band")()
    val plan = Project(Seq(alias), child)

    UdfAnalysis.internalBranches(plan, spark).map(sql) should
      contain allOf ("(amount > 1000)", "(amount > 100)")
  }
}
