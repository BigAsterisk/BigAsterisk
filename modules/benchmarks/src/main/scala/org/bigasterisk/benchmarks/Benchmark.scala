package org.bigasterisk.benchmarks

import scala.util.Random

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * One subject program from a published evaluation, expressed for a SQL front end.
 *
 * ==Why these exist==
 * Every claim in the papers is a claim about *these programs*. A platform that runs
 * only on its own fixtures cannot say whether it reproduces any of them. Each benchmark
 * here is the same computation as the upstream artifact's program — the same columns,
 * the same branch structure, the same aggregation — written as SQL rather than as an
 * RDD pipeline, because that is the surface these tools attach to here.
 *
 * ==The two fault models==
 * The papers use two, and they answer different questions:
 *
 *   - a **mutated program** (`faulty`) — BigTest's `BenchmarksFault` variants: a wrong
 *     predicate, a wrong column, a swapped key and value. This is what a technique that
 *     localises *operations* has to find.
 *   - a **corrupt record** (`corrupt`) — the SoCC subject programs' model: one bad row
 *     in an otherwise clean dataset. This is what a technique that isolates *inputs*
 *     has to find.
 *
 * A benchmark carries whichever the upstream evaluation used, and the runner skips a
 * tool whose fault model a benchmark does not provide rather than inventing one.
 */
trait Benchmark {

  /** The program's name, as the papers call it. */
  def name: String

  /** The published evaluations this program appears in. */
  def papers: Seq[String]

  /** What the program computes, in one line. */
  def summary: String

  /** Each table it reads, as a DDL string. */
  def schemas: Map[String, String]

  /** The computation, as the upstream program performs it. */
  def query: String

  /**
   * The same computation with a fault injected, in the style of the upstream fault
   * variants. `None` where the upstream evaluation planted no program fault.
   */
  def faulty: Option[String] = None

  /**
   * A record that makes the output wrong, for the input-isolation fault model.
   * `None` where the upstream evaluation planted no corrupt record.
   */
  def corrupt: Option[Map[String, Row]] = None

  /**
   * A predicate over the *output* columns that is true for a wrong row.
   *
   * This is the oracle. Without one, a technique that needs to know which results are
   * wrong cannot run, and the runner reports that rather than guessing.
   */
  def oracle: Option[String] = None

  /** Deterministic input data. The same seed must produce the same rows. */
  def rows(count: Int, random: Random): Map[String, Seq[Row]]

  /**
   * Registers this benchmark's tables and returns them.
   *
   * @param withCorrupt append the planted corrupt record, where the benchmark has one
   */
  final def register(
      spark: SparkSession,
      count: Int = 1000,
      seed: Long = 0L,
      withCorrupt: Boolean = false): Map[String, DataFrame] = {
    val generated = rows(count, new Random(seed))
    val planted = if (withCorrupt) corrupt.getOrElse(Map.empty) else Map.empty[String, Row]
    val root = new java.io.File(
      s"${Benchmark.DataDirectory}/$name-$count-$seed${if (withCorrupt) "-corrupt" else ""}")

    schemas.map { case (table, ddl) =>
      val schema = StructType.fromDDL(ddl)
      val directory = new java.io.File(root, table)

      // Written out and read back rather than parallelized in memory. The upstream
      // programs read text files, and a file scan is also what provenance capture
      // attaches to: an in-memory RDD scan is not a source it can tap, so every
      // lineage-based tool would refuse the benchmark before it began.
      if (!directory.isDirectory) {
        val data = generated.getOrElse(table, Seq.empty) ++ planted.get(table).toSeq
        spark.createDataFrame(spark.sparkContext.parallelize(data, numSlices = 2), schema)
          .write.mode("overwrite").option("header", "false")
          .csv(directory.getAbsolutePath)
      }

      val df = spark.read.schema(schema).csv(directory.getAbsolutePath)
      df.createOrReplaceTempView(table)
      table -> df
    }
  }

  /** The columns the query outputs, for writing an oracle against. */
  final def outputColumns(spark: SparkSession): Seq[String] = {
    register(spark, count = 8)
    spark.sql(query).schema.fieldNames.toSeq
  }

  override def toString: String = s"$name (${papers.mkString(", ")})"
}

object Benchmark {

  /**
   * Where generated inputs are written, for this JVM only.
   *
   * Per-run rather than cached on disk, for two reasons: a relative path means different
   * directories under `run` and under `test`, and cached data outlives a change to the
   * generator that produced it — which is how a benchmark quietly keeps measuring the
   * old thing. Generating 400 rows costs nothing.
   */
  val DataDirectory: String = sys.env.get("BENCHMARK_DATA").getOrElse {
    val temporary = java.nio.file.Files.createTempDirectory("bigasterisk-benchmarks")
    temporary.toFile.deleteOnExit()
    temporary.toAbsolutePath.toString
  }

  /** Every subject program implemented here. */
  val all: Seq[Benchmark] = Programs.all

  /** Look one up by name, case-insensitively. */
  def byName(name: String): Option[Benchmark] =
    all.find(_.name.equalsIgnoreCase(name))

  /** The benchmarks a given paper evaluated on. */
  def forPaper(paper: String): Seq[Benchmark] =
    all.filter(_.papers.exists(_.equalsIgnoreCase(paper)))
}
