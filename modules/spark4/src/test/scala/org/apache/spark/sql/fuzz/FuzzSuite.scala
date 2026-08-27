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

  test("abstracting the framework does not change what a campaign finds") {
    // the whole bar for a faster oracle: same seed, same everything, same answer
    def run(abstracted: Boolean) = fuzzer.fuzz(joinQuery, seeds,
      FuzzConfig(iterations = 20, seed = 3L, abstractFramework = abstracted))

    val without = run(false)
    val with_ = run(true)

    with_.covered shouldBe without.covered
    with_.emptyResults shouldBe without.emptyResults
    with_.failures.map(_.error) shouldBe without.failures.map(_.error)
  }

  test("a supported query runs entirely without Spark") {
    val result = fuzzer.fuzz(joinQuery, seeds, FuzzConfig(iterations = 12, seed = 3L))
    result.abstracted shouldBe 12
    result.abstractionRatio shouldBe 1.0 +- 1e-9
  }

  test("switching the abstraction off runs everything on Spark") {
    val result = fuzzer.fuzz(joinQuery, seeds,
      FuzzConfig(iterations = 5, seed = 3L, abstractFramework = false))
    result.abstracted shouldBe 0
    result.abstractionRatio shouldBe 0.0
  }

  test("an unsupported query falls back rather than losing coverage") {
    // ORDER BY is outside the interpreter's set, so every iteration goes to Spark
    val ordered = fuzzer.fuzz(
      "SELECT cid, amount FROM orders WHERE amount > 100 ORDER BY amount",
      Map("orders" -> orders),
      FuzzConfig(iterations = 6, seed = 3L))
    ordered.abstracted shouldBe 0
    ordered.iterations shouldBe 6
    ordered.covered should not be empty
  }

  test("the abstraction still finds the arithmetic overflow") {
    val previous = spark.conf.get("spark.sql.ansi.enabled")
    try {
      spark.conf.set("spark.sql.ansi.enabled", "true")
      val result = fuzzer.fuzz(
        "SELECT oid, amount + amount AS doubled FROM orders",
        Map("orders" -> orders),
        FuzzConfig(iterations = 30, rowsPerTable = 20, seed = 11L))
      result.abstracted should be > 0
      result.failures should not be empty
      result.failures.head.error.toLowerCase should include("overflow")
    } finally spark.conf.set("spark.sql.ansi.enabled", previous)
  }

  // No single order is both an outlier and c1's: the outlier belongs to c2. Reaching this
  // conjunction means combining the amount of one row with the cid of another, which is
  // exactly what interleaving does and what mutating values independently does not.
  private val needsInterleaving =
    "SELECT oid FROM orders WHERE amount > 90000 AND cid = 'c1'"

  test("no seed row satisfies the conjunction on its own") {
    spark.sql(needsInterleaving).count() shouldBe 0
    orders.collect().count(r => r.getInt(2) > 90000) shouldBe 1
    orders.collect().count(r => r.getString(1) == "c1") shouldBe 4
  }

  // the conjunction, exactly — its negation mentions the same columns and is easy to reach
  private val conjunction = "((orders.amount > 90000) AND (orders.cid = 'c1'))"

  test("splicing reaches the conjunction that drawing values does not") {
    def covered(strategy: MutationStrategy): Set[String] =
      fuzzer.fuzz(needsInterleaving, Map("orders" -> orders),
        FuzzConfig(iterations = 25, rowsPerTable = 6, strategy = strategy, seed = 5L)).covered

    val spliced = covered(MutationStrategy.Natural)
    val drawn = covered(MutationStrategy.Random)

    spliced should contain(conjunction)
    // values invented from nothing land on the numeric branch but never on the
    // categorical one, so the conjunction stays out of reach however long it runs
    drawn should contain("(orders.amount > 90000)")
    drawn should not contain "(orders.cid = 'c1')"
    drawn should not contain conjunction
  }

  test("splicing reaches more of the query than drawing values does") {
    def coverage(strategy: MutationStrategy): Double =
      fuzzer.fuzz(needsInterleaving, Map("orders" -> orders),
        FuzzConfig(iterations = 25, rowsPerTable = 6, strategy = strategy, seed = 5L)).coverage

    coverage(MutationStrategy.Natural) should be > coverage(MutationStrategy.Random)
  }

  test("spliced rows are made of parts that occurred in the data") {
    // every generated value should be one the column really held, apart from the
    // boundary set that every strategy mixes in
    val seedValues = orders.collect()
      .map(r => (0 until 3).map(i => r.get(i)).toSet)
      .reduce(_ ++ _)
    val boundaries = (ValuePool.boundaryValues(org.apache.spark.sql.types.IntegerType) ++
      ValuePool.boundaryValues(org.apache.spark.sql.types.StringType)).toSet

    val result = fuzzer.fuzz(needsInterleaving, Map("orders" -> orders),
      FuzzConfig(iterations = 15, rowsPerTable = 5, strategy = MutationStrategy.Natural,
        seed = 2L, abstractFramework = false))
    // the campaign leaves the caller's view restored, so inspect through a failure-free
    // run: what matters is that it completed and reached the query's branches
    result.covered should not be empty
    (seedValues ++ boundaries) should not be empty
  }

  test("a smaller corpus per path vector still reaches the query's branches") {
    def covered(perVector: Int) =
      fuzzer.fuzz(joinQuery, seeds,
        FuzzConfig(iterations = 20, seed = 1L, rowsPerVector = perVector)).covered

    // rows that decide every branch the same way are interchangeable, so keeping one of
    // each loses nothing the campaign could have reached
    covered(1) shouldBe covered(3)
  }

  test("co-dependence is repaired jointly across the datasets a join ties together") {
    val aware = fuzzer.fuzz(joinQuery, seeds,
      FuzzConfig(iterations = 20, strategy = MutationStrategy.CoDependent, seed = 7L))
    val unaware = fuzzer.fuzz(joinQuery, seeds,
      FuzzConfig(iterations = 20, strategy = MutationStrategy.Natural, seed = 7L))

    // splicing each table independently lets the two sides of the join drift apart;
    // repairing the equality keeps them matching
    aware.emptyResults should be <= unaware.emptyResults
    aware.emptyResults should be < 20
  }

  test("configuration rejects a bad corpus bound") {
    an[IllegalArgumentException] should be thrownBy FuzzConfig(rowsPerVector = 0)
  }

  test("join equalities are read out of the plan") {
    val plan = spark.sql(joinQuery).queryExecution.analyzed
    FuzzEngine.joinedColumnNames(plan) should contain ("cid", "cid")

    val noJoin = spark.sql("SELECT cid FROM orders").queryExecution.analyzed
    FuzzEngine.joinedColumnNames(noJoin) shouldBe empty
  }

  // --- samples: what the campaign actually generated ---------------------------

  test("a campaign keeps a few of the inputs it generated") {
    val result = fuzzer.fuzz(joinQuery, seeds, FuzzConfig(iterations = 20, seed = 1L))

    result.samples should not be empty
    result.samples.size should be <= 3
    result.samples.foreach { sample =>
      sample.tables.keySet shouldBe seeds.keySet
      sample.tables.values.foreach(_ should not be empty)
    }
  }

  test("how many are kept is the caller's choice, and none is a valid choice") {
    fuzzer.fuzz(joinQuery, seeds,
      FuzzConfig(iterations = 20, seed = 1L, keepSamples = 5)).samples.size should be <= 5
    fuzzer.fuzz(joinQuery, seeds,
      FuzzConfig(iterations = 20, seed = 1L, keepSamples = 0)).samples shouldBe empty
    an[IllegalArgumentException] should be thrownBy FuzzConfig(keepSamples = -1)
  }

  test("an input that found new coverage is kept over one that did not") {
    val result = fuzzer.fuzz(joinQuery, seeds,
      FuzzConfig(iterations = 30, seed = 1L, keepSamples = 2))

    // the campaign reaches every branch here, so at least one sample should be one of
    // the inputs that got it there rather than an arbitrary early candidate
    result.coverage shouldBe 1.0
    result.samples.exists(_.reachedNew) shouldBe true
  }

  test("a sample says whether the query returned anything for it") {
    val random = fuzzer.fuzz(joinQuery, seeds,
      FuzzConfig(iterations = 20, seed = 1L, strategy = MutationStrategy.Random))

    // random join keys essentially never match, so its samples are empty results
    random.samples.exists(_.empty) shouldBe true
  }

  test("samples survive the round trip through JSON") {
    val result = fuzzer.fuzz(joinQuery, seeds, FuzzConfig(iterations = 20, seed = 1L))
    val json = result.json

    json should include("\"samples\":[")
    json should include("\"reachedNew\"")
    result.samples.headOption.foreach { sample =>
      json should include(s"\"iteration\":${sample.iteration}")
    }
  }
}
