package org.apache.spark.sql.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import org.apache.spark.sql.functions.{col, udf}

import org.bigasterisk.api.{BigAsterisk, UdfProfile}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Reading a Scala UDF's branches out of its bytecode.
 *
 * The functions here are ordinary Scala lambdas, compiled by the same compiler that
 * compiles a user's — which is the point: nothing about them is prepared for analysis.
 */
class ScalaUdfAnalysisSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = BigAsterisk.configure(
      SparkSession.builder()
        .master("local[2]")
        .appName("scala-udf-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def orders =
    spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("src/test/resources/orders_csv")

  /** The `ScalaUDF` node Spark leaves in the plan for `function` applied to `columns`. */
  private def analyse(
      function: org.apache.spark.sql.expressions.UserDefinedFunction,
      columns: String*): Option[UdfProfile] = {
    val projected = orders.select(function(columns.map(col): _*).as("result"))
    val node = projected.queryExecution.analyzed.expressions
      .flatMap(_.collect { case s: ScalaUDF => s })
      .headOption
      .getOrElse(fail("no ScalaUDF in the plan"))
    ScalaUdfAnalysis.profile(node)
  }

  test("a single comparison becomes a branch over the argument") {
    val classify = udf((amount: Int) => if (amount > 1000) "high" else "low")
    val profile = analyse(classify, "amount").getOrElse(fail("no profile"))

    profile.parameters shouldBe Seq("arg0")
    profile.branches.map(_.condition) should contain("(arg0 > 1000)")
    profile.paths should have size 2
    profile.paths.flatMap(_.returns) should contain allOf ("'high'", "'low'")
  }

  test("the paths say which constant each produces") {
    val classify = udf((amount: Int) => if (amount > 1000) "high" else "low")
    val profile = analyse(classify, "amount").get

    val high = profile.paths.find(_.returns.contains("'high'")).getOrElse(fail("no high path"))
    high.constraint should include("arg0 > 1000")
    high.exact shouldBe true

    // the opposite arm inverts the operator rather than wrapping it in NOT, so a branch
    // and its negation are not two spellings of the same condition
    val low = profile.paths.find(_.returns.contains("'low'")).get
    low.constraint should include("arg0 <= 1000")
  }

  test("a chain of comparisons yields a path each") {
    val band = udf((amount: Int) =>
      if (amount > 1000) "high" else if (amount > 100) "medium" else "low")
    val profile = analyse(band, "amount").get

    profile.branches.size should be >= 2
    profile.paths.flatMap(_.returns).toSet shouldBe Set("'high'", "'medium'", "'low'")
    profile.isSolvable shouldBe true
  }

  test("the argument that decides the result is reported as influencing") {
    val classify = udf((amount: Int) => if (amount > 1000) "high" else "low")
    analyse(classify, "amount").get.influencing shouldBe Set("arg0")
  }

  test("an argument that cannot change the result is not reported") {
    // `cid` is passed and never read
    val ignoring = udf((amount: Int, cid: String) => if (amount > 1000) "high" else "low")
    val profile = analyse(ignoring, "amount", "cid").get

    profile.parameters shouldBe Seq("arg0", "arg1")
    profile.influencing shouldBe Set("arg0")
    profile.influences(0) shouldBe true
    profile.influences(1) shouldBe false
  }

  test("a branch whose arms return the same value influences nothing") {
    val pointless = udf((amount: Int) => if (amount > 1000) "same" else "same")
    analyse(pointless, "amount").get.influencing shouldBe empty
  }

  test("a string comparison is read") {
    val vip = udf((cid: String) => if (cid == "c1") "vip" else "normal")
    val profile = analyse(vip, "cid").get

    profile.branches.map(_.condition).mkString should include("c1")
    profile.paths.flatMap(_.returns).toSet shouldBe Set("'vip'", "'normal'")
  }

  test("startsWith is kept as a string predicate") {
    val prefixed = udf((oid: String) => if (oid.startsWith("o1")) "early" else "late")
    val profile = analyse(prefixed, "oid").get

    profile.branches.map(_.condition).mkString should include("startswith")
  }

  test("a null test is read") {
    val missing = udf((cid: String) => if (cid == null) "missing" else "present")
    val profile = analyse(missing, "cid").get

    profile.branches.map(_.condition).mkString.toUpperCase should include("NULL")
  }

  test("arithmetic on the argument is carried into the condition") {
    val doubled = udf((amount: Int) => if (amount * 2 > 100) "big" else "small")
    val profile = analyse(doubled, "amount").get

    profile.branches.map(_.condition).mkString should include("*")
  }

  test("a function with no branch is a single path") {
    val doubling = udf((amount: Int) => amount * 2)
    val profile = analyse(doubling, "amount").get

    profile.branches shouldBe empty
    profile.paths should have size 1
    profile.paths.head.constraint shouldBe "true"
    // the value is computed, so a caller cannot invert it
    profile.paths.head.returns shouldBe empty
  }

  test("a captured value is folded in as the constant it is") {
    val threshold = 500
    val over = udf((amount: Int) => if (amount > threshold) "over" else "under")
    val profile = analyse(over, "amount").get

    profile.parameters shouldBe Seq("arg0")
    profile.branches.map(_.condition).mkString should include("500")
  }

  test("a call the analysis does not model is reported, not guessed at") {
    val opaque = udf((cid: String) => if (cid.hashCode % 2 == 0) "even" else "odd")
    val profile = analyse(opaque, "cid").get

    profile.isComplete shouldBe false
    profile.unsupported should not be empty
    profile.isSolvable shouldBe false
  }

  test("analysis is cached, so a repeated query does not re-read the bytecode") {
    val classify = udf((amount: Int) => if (amount > 1000) "high" else "low")
    val first = analyse(classify, "amount").get
    val second = analyse(classify, "amount").get
    first shouldBe second
  }

  // --- the payoff: the branches reach the tools, not just the profile ------------

  test("a branch inside a Scala UDF becomes a condition on the query's columns") {
    val classify = udf((amount: Int) => if (amount > 1000) "high" else "low")
    val plan = orders.select(classify(col("amount")).as("band"))
      .queryExecution.analyzed

    val bound = UdfAnalysis.internalBranches(
      plan, spark.asInstanceOf[org.apache.spark.sql.classic.SparkSession])

    bound.map(_.sql) should contain("(amount > 1000)")
  }

  test("testing a Scala UDF's result becomes a condition on its argument") {
    val classify = udf((amount: Int) => if (amount > 1000) "high" else "low")
    val classic = spark.asInstanceOf[org.apache.spark.sql.classic.SparkSession]
    // one DataFrame for both sides: a UDF taken from a second read of the same file
    // carries different expression ids and could not resolve against this plan
    val source = orders
    val child = source.queryExecution.analyzed
    val condition = org.apache.spark.sql.catalyst.expressions.EqualTo(
      source.select(classify(col("amount"))).queryExecution.analyzed.expressions
        .flatMap(_.collect { case s: ScalaUDF => s }).head,
      org.apache.spark.sql.catalyst.expressions.Literal("high"))

    UdfAnalysis.solveThrough(condition, child, classic).map(_.map(_.sql)) shouldBe
      Some(Seq("(amount > 1000)"))
  }

  test("influence narrows to the argument a Scala UDF actually reads") {
    val ignoring = udf((amount: Int, cid: String) => if (amount > 1000) "high" else "low")
    val call = orders.select(ignoring(col("amount"), col("cid")))
      .queryExecution.analyzed.expressions
      .flatMap(_.collect { case s: ScalaUDF => s }).head

    call.references.map(_.name).toSet shouldBe Set("amount", "cid")
    UdfAnalysis.influencing(call).map(_.name) shouldBe Set("amount")
  }

  test("an unreadable Scala UDF stays a black box, implicating every argument") {
    val opaque = udf((amount: Int, cid: String) => cid.hashCode + amount)
    val call = orders.select(opaque(col("amount"), col("cid")))
      .queryExecution.analyzed.expressions
      .flatMap(_.collect { case s: ScalaUDF => s }).head

    // the profile is partial, so nothing is narrowed away
    UdfAnalysis.influencing(call).map(_.name) shouldBe Set("amount", "cid")
  }
}
