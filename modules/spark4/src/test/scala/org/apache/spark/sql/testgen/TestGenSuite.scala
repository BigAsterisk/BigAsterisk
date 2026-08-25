package org.apache.spark.sql.testgen

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.IntegerType

import org.bigasterisk.api.{BigAsterisk, TestGenConfig}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TestGenSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = BigAsterisk.configure(
      SparkSession.builder()
        .master("local[2]")
        .appName("testgen-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def generator = BigAsterisk.testgen(spark)

  private def orders: DataFrame = {
    val df = spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("src/test/resources/orders_csv")
    df.createOrReplaceTempView("orders")
    df
  }

  private def seeds = Map("orders" -> orders)

  test("both sides of a filter are generated and verified") {
    val suite = generator.generate("SELECT cid FROM orders WHERE amount > 100", seeds)

    suite.cases should have size 2
    suite.verified should not be empty

    // the input built to take the branch really does contain a qualifying row
    val taking = suite.cases.find(!_.path.startsWith("NOT")).get
    taking.verified shouldBe true
    taking.note shouldBe "verified"
    taking.tables("orders").exists(_.getInt(2) > 100) shouldBe true
  }

  test("the not-taken side contains no qualifying row") {
    val suite = generator.generate("SELECT cid FROM orders WHERE amount > 100", seeds)
    val avoiding = suite.cases.find(_.path.startsWith("NOT")).get
    avoiding.tables("orders").foreach { row =>
      if (!row.isNullAt(2)) row.getInt(2) should be <= 100
    }
  }

  test("a solved bound is respected exactly at the boundary") {
    // > 100 on an integer column means the smallest satisfying value is 101
    val suite = generator.generate(
      "SELECT cid FROM orders WHERE amount > 100",
      Map("orders" -> orders),
      TestGenConfig(natural = false, rowsPerPath = 1))
    val taking = suite.cases.find(!_.path.startsWith("NOT")).get
    taking.tables("orders").head.getInt(2) should be >= 101
  }

  test("a conjunction of bounds is solved to the intersection") {
    val suite = generator.generate(
      "SELECT cid FROM orders WHERE amount > 100 AND amount < 200",
      Map("orders" -> orders),
      TestGenConfig(natural = false, rowsPerPath = 1))
    val taking = suite.cases.find(c => c.verified && !c.path.startsWith("NOT"))
    taking shouldBe defined
    val amount = taking.get.tables("orders").head.getInt(2)
    amount should be > 100
    amount should be < 200
  }

  test("an unsatisfiable path is reported rather than fabricated") {
    val suite = generator.generate(
      "SELECT cid FROM orders WHERE amount > 200 AND amount < 100", seeds)
    val impossible = suite.cases.filter(c => !c.verified && c.note == "unsatisfiable")
    impossible should not be empty
    impossible.foreach(_.tables shouldBe empty)
  }

  test("naturalness draws witnesses from values that really occur") {
    val observed = orders.collect().map(_.getInt(2)).toSet
    val suite = generator.generate(
      "SELECT cid FROM orders WHERE amount > 100",
      seeds,
      TestGenConfig(natural = true, rowsPerPath = 1))
    val taking = suite.cases.find(c => c.verified && !c.path.startsWith("NOT")).get
    // the constraint is satisfiable by real data, so a real value should be chosen
    observed should contain(taking.tables("orders").head.getInt(2))
  }

  test("without naturalness the witness is synthesised, not borrowed") {
    val suite = generator.generate(
      "SELECT cid FROM orders WHERE amount > 100",
      seeds,
      TestGenConfig(natural = false, rowsPerPath = 1))
    val taking = suite.cases.find(!_.path.startsWith("NOT")).get
    // 101 is the boundary the solver derives; no order in the seed data has it
    taking.tables("orders").head.getInt(2) shouldBe 101
  }

  test("equality on a string column is solved") {
    val suite = generator.generate(
      "SELECT amount FROM orders WHERE cid = 'c2'",
      seeds,
      TestGenConfig(rowsPerPath = 1))
    val taking = suite.cases.find(c => c.verified && !c.path.startsWith("NOT"))
    taking shouldBe defined
    taking.get.tables("orders").head.getString(1) shouldBe "c2"
  }

  test("a condition outside the solver's fragment is reported, not guessed") {
    // a comparison between two columns, which the interval solver cannot express
    val suite = generator.generate(
      "SELECT cid FROM orders o WHERE o.amount > LENGTH(o.oid) * 100", seeds)
    suite.cases.foreach { c =>
      if (!c.verified) c.note should not be empty
    }
    // whatever happens, nothing claims verification it did not achieve
    suite.verified.foreach(_.note shouldBe "verified")
  }

  test("every case reports honestly whether its path was reached") {
    val suite = generator.generate("SELECT cid FROM orders WHERE amount > 100", seeds)
    suite.cases.foreach { c =>
      if (c.verified) c.note shouldBe "verified"
      else c.note should not be "verified"
    }
  }

  test("path enumeration is bounded, falling back to branch coverage") {
    val suite = generator.generate(
      """SELECT cid FROM orders
        |WHERE amount > 100 AND amount < 500 AND cid <> 'c9' AND amount <> 7""".stripMargin,
      seeds,
      TestGenConfig(maxPaths = 4))
    suite.cases.size should be <= 4
  }

  test("coverage counts only paths a verified test reached") {
    val suite = generator.generate("SELECT cid FROM orders WHERE amount > 100", seeds)
    suite.coverage should be > 0.0
    suite.coverage should be <= 1.0
    suite.totalBranches should be > 0
  }

  test("a query with no branches yields an empty suite") {
    val suite = generator.generate("SELECT cid, amount FROM orders", seeds)
    suite.cases shouldBe empty
    suite.totalBranches shouldBe 0
    suite.coverage shouldBe 1.0
  }

  test("generation is reproducible from its seed") {
    def run() = generator.generate("SELECT cid FROM orders WHERE amount > 100", seeds,
      TestGenConfig(seed = 5L)).cases.map(c => (c.path, c.verified))
    run() shouldBe run()
  }

  test("the caller's views are restored afterwards") {
    val before = spark.table("orders").count()
    generator.generate("SELECT cid FROM orders WHERE amount > 100", seeds)
    spark.table("orders").count() shouldBe before
  }

  test("configuration rejects nonsense") {
    an[IllegalArgumentException] should be thrownBy TestGenConfig(maxPaths = 0)
    an[IllegalArgumentException] should be thrownBy TestGenConfig(rowsPerPath = 0)
    an[IllegalArgumentException] should be thrownBy generator.generate("SELECT 1", Map.empty)
  }

  test("the domain solver handles bounds, equality and contradiction") {
    val int = ColumnDomain(IntegerType)

    // > 100 on an integer means >= 101
    int.withLower(100, inclusive = false).witness(IndexedSeq.empty, new scala.util.Random(0))
      .map(_.asInstanceOf[Int]) shouldBe Some(101)

    // intersecting bounds
    val narrowed = int.withLower(100, inclusive = false).withUpper(103, inclusive = true)
    narrowed.satisfies(101) shouldBe true
    narrowed.satisfies(100) shouldBe false
    narrowed.satisfies(104) shouldBe false

    // contradiction is detected rather than producing a bogus witness
    val impossible = int.withLower(200, inclusive = true).withUpper(100, inclusive = true)
    impossible.isUnsatisfiable shouldBe true
    impossible.witness(IndexedSeq.empty, new scala.util.Random(0)) shouldBe None

    // null constraints cannot both hold
    ColumnDomain(IntegerType, mustBeNull = true, mustNotBeNull = true)
      .isUnsatisfiable shouldBe true

    // a natural value is preferred when it fits
    narrowed.witness(IndexedSeq(102), new scala.util.Random(0)) shouldBe Some(102)
    // and ignored when it does not
    narrowed.witness(IndexedSeq(5), new scala.util.Random(0)) should not be Some(5)
  }
}
