package org.bigasterisk.benchmarks

import org.apache.spark.sql.SparkSession

import org.bigasterisk.api.BigAsterisk

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The subject programs themselves: that each one runs, produces output, and carries a
 * fault its oracle actually detects.
 *
 * A benchmark whose oracle never fires measures nothing while looking like it does, so
 * these are checks on the benchmarks rather than on the tools.
 */
class BenchmarkSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = BigAsterisk.configure(
      SparkSession.builder()
        .master("local[2]")
        .appName("benchmark-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private val rows = 120

  test("every program is attributed to at least one published evaluation") {
    Benchmark.all should not be empty
    Benchmark.all.foreach { b =>
      withClue(b.name) {
        b.papers should not be empty
        b.summary should not be empty
        b.schemas should not be empty
      }
    }
  }

  test("program names are unique and findable") {
    Benchmark.all.map(_.name.toLowerCase).distinct should have size Benchmark.all.size
    Benchmark.byName("commutetype") shouldBe defined
    Benchmark.byName("nonexistent") shouldBe empty
  }

  test("each paper's programs can be listed") {
    Benchmark.forPaper("DepFuzz").map(_.name) should contain("CommuteType")
    Benchmark.forPaper("NaturalFuzz").map(_.name) should contain("CommuteTypeFull")
    Benchmark.forPaper("nonexistent") shouldBe empty
  }

  test("every program runs and produces output") {
    Benchmark.all.foreach { b =>
      withClue(s"${b.name}: ") {
        b.register(spark, rows, seed = 3L)
        val result = spark.sql(b.query).collect()
        result should not be empty
      }
    }
  }

  test("generated data is deterministic in the seed") {
    Benchmark.all.foreach { b =>
      withClue(s"${b.name}: ") {
        val first = b.rows(rows, new scala.util.Random(7L))
        val second = b.rows(rows, new scala.util.Random(7L))
        first.keySet shouldBe second.keySet
        first.foreach { case (table, data) => data shouldBe second(table) }
      }
    }
  }

  test("generated tables match their declared schemas") {
    Benchmark.all.foreach { b =>
      withClue(s"${b.name}: ") {
        val tables = b.register(spark, rows, seed = 3L)
        tables.keySet shouldBe b.schemas.keySet
        tables.foreach { case (name, df) =>
          df.count() should be > 0L
          df.schema.fieldNames.toSeq shouldBe
            org.apache.spark.sql.types.StructType.fromDDL(b.schemas(name)).fieldNames.toSeq
        }
      }
    }
  }

  test("an injected program fault really changes the answer") {
    val withFaults = Benchmark.all.filter(_.faulty.isDefined)
    withFaults should not be empty

    withFaults.foreach { b =>
      withClue(s"${b.name}: ") {
        b.register(spark, rows, seed = 3L)
        val correct = spark.sql(b.query).collect().map(_.toSeq).toSet
        val faulty = spark.sql(b.faulty.get).collect().map(_.toSeq).toSet
        // a fault that produces the same output would make the benchmark measure nothing
        faulty should not be correct
      }
    }
  }

  test("a planted corrupt record really makes the output wrong") {
    val withCorrupt = Benchmark.all.filter(b => b.corrupt.isDefined && b.oracle.isDefined)
    withCorrupt should not be empty

    withCorrupt.foreach { b =>
      withClue(s"${b.name}: ") {
        b.register(spark, rows, seed = 3L, withCorrupt = false)
        val clean = spark.sql(b.query).collect()
        val test = Oracle.compile(spark, b.query, b.oracle.get)
        clean.count(test) shouldBe 0          // the oracle must not fire on clean data

        b.register(spark, rows, seed = 3L, withCorrupt = true)
        spark.sql(b.query).collect().count(test) should be > 0
      }
    }
  }

  test("the oracle compiles with type coercion rather than failing at evaluation") {
    // `delta > 6000` over a double column needs a cast on the literal
    val weather = Benchmark.byName("WeatherAnalysis").get
    weather.register(spark, rows, seed = 3L, withCorrupt = true)
    val test = Oracle.compile(spark, weather.query, weather.oracle.get)
    noException should be thrownBy spark.sql(weather.query).collect().foreach(test)
  }

  test("an oracle naming a column the query does not output is rejected") {
    val weather = Benchmark.byName("WeatherAnalysis").get
    weather.register(spark, rows, seed = 3L)
    an[Exception] should be thrownBy
      Oracle.compile(spark, weather.query, "nonexistent > 1")
  }

  test("the differential oracle marks exactly the rows the fault changed") {
    val movie = Benchmark.byName("MovieRating").get
    movie.register(spark, rows, seed = 3L)

    val wrong = Oracle.differential(spark, movie.query)
    spark.sql(movie.query).collect().count(wrong) shouldBe 0

    val faulty = spark.sql(movie.faulty.get).collect()
    faulty.count(wrong) should be > 0
  }

  test("the harness measures every program without an error") {
    // one program, so the suite stays quick; the full sweep is the runner's job
    val commute = Benchmark.byName("StudentGrade").get
    val measurements = BenchmarkRunner.measure(spark, commute)

    measurements should not be empty
    withClue(measurements.filterNot(_.isOk).mkString("\n")) {
      measurements.count(_.status == "error") shouldBe 0
    }
    measurements.map(_.tool) should contain allOf ("BigTest", "Fuzzing", "BigSift", "Titian")
  }
}
