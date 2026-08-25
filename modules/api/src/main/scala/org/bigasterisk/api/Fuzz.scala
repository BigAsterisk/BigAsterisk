package org.bigasterisk.api

import org.apache.spark.sql.{DataFrame, Row}

/**
 * How new test inputs are derived from the ones already seen.
 *
 * The three strategies are the three fuzzers this platform unifies. They differ only in
 * how a candidate row is built, which is why they share one loop rather than three.
 *
 * @group fuzz
 */
sealed trait MutationStrategy { def name: String }

object MutationStrategy {

  /**
   * Values drawn at random for each column's type.
   *
   * The baseline: cheap, and good at finding crashes on malformed values. Poor at
   * getting past a join, since a randomly generated key almost never matches.
   * Corresponds to BigFuzz.
   */
  case object Random extends MutationStrategy { val name = "random" }

  /**
   * Values spliced column-wise out of rows already seen.
   *
   * Every value is one that genuinely occurred in that column, so generated rows look
   * like real data — the right shape, the right formats, plausible magnitudes — while
   * the *combinations* are new. Corresponds to NaturalFuzz.
   */
  case object Natural extends MutationStrategy { val name = "natural" }

  /**
   * Splicing that respects co-dependence between tables.
   *
   * A join makes two columns of two tables dependent: mutate one freely and the rows
   * stop matching, so the query returns nothing and the fuzzer learns nothing. This
   * draws joined columns from the same pool on both sides, so generated rows survive
   * the join. Corresponds to DepFuzz.
   */
  case object CoDependent extends MutationStrategy { val name = "co-dependent" }

  val all: Seq[MutationStrategy] = Seq(Random, Natural, CoDependent)

  /** Looks a strategy up by [[MutationStrategy.name]]. */
  def byName(name: String): MutationStrategy =
    all.find(_.name.equalsIgnoreCase(name)).getOrElse {
      throw new IllegalArgumentException(
        s"unknown mutation strategy '$name'; expected one of ${all.map(_.name).mkString(", ")}")
    }
}

/**
 * How a fuzzing campaign is run.
 *
 * @param iterations   how many candidate inputs to try
 * @param strategy     how candidates are derived
 * @param rowsPerTable rows to generate for each table
 * @param seed         random seed, so a campaign reproduces exactly
 * @param guided       when true, a candidate that reaches a branch nothing had reached
 *                     is kept and mutated further; when false every candidate is drawn
 *                     from the seed data alone
 * @param abstractFramework when true, iterations are evaluated by interpreting the
 *                     query's plan over in-memory rows instead of running a Spark job.
 *                     The operator semantics are Spark's either way; what goes is the
 *                     planning, scheduling and task setup that dwarfs a twenty-row query.
 *                     Any query the interpreter does not support falls back to Spark, so
 *                     this changes speed and never results.
 *
 * @group fuzz
 */
case class FuzzConfig(
    iterations: Int = 100,
    strategy: MutationStrategy = MutationStrategy.CoDependent,
    rowsPerTable: Int = 10,
    seed: Long = 0L,
    guided: Boolean = true,
    abstractFramework: Boolean = true) {
  require(iterations >= 0, s"iterations must not be negative, got $iterations")
  require(rowsPerTable > 0, s"rowsPerTable must be positive, got $rowsPerTable")
}

/**
 * An input that made the query fail.
 *
 * @param iteration when it was found
 * @param error     the exception's type and message
 * @param tables    the generated rows, by table name — enough to reproduce the failure
 *
 * @group fuzz
 */
case class FuzzFailure(iteration: Int, error: String, tables: Map[String, Seq[Row]]) {
  override def toString: String =
    s"iteration $iteration: $error\n" +
      tables.map { case (t, rows) => s"  $t: ${rows.mkString(", ")}" }.mkString("\n")
}

/**
 * What a fuzzing campaign found.
 *
 * @param iterations      candidates actually run
 * @param failures        inputs that made the query fail
 * @param covered         branches of the query that some input reached
 * @param totalBranches   branches the query has
 * @param emptyResults    candidates that produced no output rows at all — the symptom
 *                        of inputs that cannot get past a join
 * @param abstracted      iterations evaluated without Spark. The rest fell back, either
 *                        because the abstraction was switched off or because the query
 *                        left the interpreter's supported set.
 *
 * @group fuzz
 */
case class FuzzResult(
    iterations: Int,
    failures: Seq[FuzzFailure],
    covered: Set[String],
    totalBranches: Int,
    emptyResults: Int,
    abstracted: Int = 0) {

  /** Fraction of iterations that avoided Spark entirely. */
  def abstractionRatio: Double =
    if (iterations == 0) 0.0 else abstracted.toDouble / iterations

  /** Fraction of the query's branches that some generated input reached. */
  def coverage: Double = if (totalBranches == 0) 1.0 else covered.size.toDouble / totalBranches

  override def toString: String =
    f"FuzzResult($iterations iterations ($abstracted without Spark), " +
      f"${failures.size} failures, coverage ${coverage * 100}%.0f%% of $totalBranches " +
      f"branches, $emptyResults empty)"

  /**
   * The result as a JSON object.
   *
   * Provided for language bindings that cannot marshal Scala collections across the
   * process boundary — the PySpark front end reads this. Generated rows are rendered
   * with their `toString`, which is enough to reproduce a failure by eye.
   */
  def json: String = {
    def quote(s: String): String =
      "\"" + s.flatMap {
        case '"'              => "\\\""
        case '\\'             => "\\\\"
        case '\n'             => "\\n"
        case '\r'             => "\\r"
        case '\t'             => "\\t"
        case c if c.isControl => f"\\u${c.toInt}%04x"
        case c                => c.toString
      } + "\""

    val failuresJson = failures.map { f =>
      val tables = f.tables.map { case (name, rows) =>
        s"${quote(name)}:[${rows.map(r => quote(r.toString)).mkString(",")}]"
      }.mkString(",")
      s"""{"iteration":${f.iteration},"error":${quote(f.error)},"tables":{$tables}}"""
    }.mkString(",")

    s"""{"iterations":$iterations,"totalBranches":$totalBranches,""" +
      s""""emptyResults":$emptyResults,"coverage":$coverage,"abstracted":$abstracted,""" +
      s""""covered":[${covered.toSeq.sorted.map(quote).mkString(",")}],""" +
      s""""failures":[$failuresJson]}"""
  }
}

/**
 * Fuzz testing for Spark SQL: generate inputs for a query and see what breaks.
 *
 * Testing a data-intensive application normally means running it over a real dataset,
 * which is slow and covers only the cases that dataset happens to contain. Fuzzing
 * generates small inputs instead, and steers them toward parts of the query nothing has
 * exercised yet.
 *
 * Obtain an instance from [[BigAsterisk.fuzz]].
 *
 * {{{
 * val result = BigAsterisk.fuzz(spark).fuzz(
 *   "SELECT c.name, SUM(o.amount) FROM orders o JOIN customers c ON o.cid = c.cid " +
 *   "GROUP BY c.name",
 *   Map("orders" -> orders, "customers" -> customers))
 *
 * result.coverage          // how much of the query the campaign reached
 * result.failures.foreach(println)
 * }}}
 *
 * @group fuzz
 */
trait FuzzSupport {

  /**
   * Runs a fuzzing campaign against `query`.
   *
   * @param seeds the tables the query reads, by the name it reads them under. Their
   *        rows are the corpus generated values are drawn from; only the schema is
   *        required for [[MutationStrategy.Random]].
   */
  def fuzz(query: String, seeds: Map[String, DataFrame], config: FuzzConfig): FuzzResult

  /** Runs a campaign with the default configuration. */
  final def fuzz(query: String, seeds: Map[String, DataFrame]): FuzzResult =
    fuzz(query, seeds, FuzzConfig())

  /**
   * [[fuzz]] for callers that cannot build a Scala `Map` or a [[FuzzConfig]] — Java,
   * and Py4J, which marshals a Python dict to `java.util.Map`.
   */
  final def fuzzJava(
      query: String,
      seeds: java.util.Map[String, DataFrame],
      iterations: Int,
      strategy: String,
      rowsPerTable: Int,
      seed: Long,
      guided: Boolean,
      abstractFramework: Boolean): FuzzResult = {
    import scala.jdk.CollectionConverters._
    fuzz(
      query,
      seeds.asScala.toMap,
      FuzzConfig(iterations, MutationStrategy.byName(strategy), rowsPerTable, seed, guided,
        abstractFramework))
  }
}
