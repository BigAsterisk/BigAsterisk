package org.bigasterisk.api

import org.apache.spark.sql.{DataFrame, Row}

/**
 * How a test-generation run is configured.
 *
 * @param maxPaths  the most paths to enumerate. Paths grow exponentially with the
 *                  number of branches, so this is a real bound, not a formality.
 * @param rowsPerPath rows to generate per table for each path
 * @param natural   when true, witnesses are drawn from values that actually occur in
 *                  the seed data wherever a constraint allows it, so generated tests
 *                  look like real records rather than like solver output. This is what
 *                  separates NaturalSym from BigTest.
 * @param seed      random seed, so a run reproduces exactly
 *
 * @group testgen
 */
case class TestGenConfig(
    maxPaths: Int = 32,
    rowsPerPath: Int = 3,
    natural: Boolean = true,
    seed: Long = 0L) {
  require(maxPaths > 0, s"maxPaths must be positive, got $maxPaths")
  require(rowsPerPath > 0, s"rowsPerPath must be positive, got $rowsPerPath")
}

/**
 * One generated test: an input built to drive the query down a particular path.
 *
 * @param id       position in the generated suite
 * @param path     the branch outcomes this input was built to take, as SQL text
 * @param tables   the generated rows, by table name
 * @param verified whether running the query with these rows actually took that path.
 *                 A generator that reports coverage it did not achieve is worse than
 *                 useless, so every test is executed and checked.
 * @param note     what happened — `"verified"`, `"unsatisfiable"`, or why the path was
 *                 not reached
 *
 * @group testgen
 */
case class TestCase(
    id: Int,
    path: String,
    tables: Map[String, Seq[Row]],
    verified: Boolean,
    note: String) {

  override def toString: String = {
    val mark = if (verified) "ok" else "--"
    val rows = tables.map { case (t, r) => s"    $t: ${r.mkString(", ")}" }.mkString("\n")
    s"[$mark] $path  ($note)\n$rows"
  }
}

/**
 * What a test-generation run produced.
 *
 * @group testgen
 */
case class TestSuite(cases: Seq[TestCase], totalBranches: Int) {

  /** Tests whose path was reached when the input was actually run. */
  def verified: Seq[TestCase] = cases.filter(_.verified)

  /** Branches some verified test reached. */
  def coveredBranches: Set[String] =
    verified.flatMap(_.path.split(" AND ").map(_.trim)).toSet

  /** Fraction of the query's branches a verified test reaches. */
  def coverage: Double =
    if (totalBranches == 0) 1.0
    else math.min(1.0, coveredBranches.count(!_.startsWith("NOT ")).toDouble / totalBranches)

  override def toString: String =
    s"TestSuite(${cases.size} cases, ${verified.size} verified, " +
      s"$totalBranches branches)"

  /**
   * The suite as a JSON object.
   *
   * Provided for language bindings that cannot marshal Scala collections across the
   * process boundary — the PySpark front end reads this. Generated rows are rendered
   * with their `toString`, which is what a test case needs to be readable.
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

    val casesJson = cases.map { c =>
      val tables = c.tables.map { case (name, rows) =>
        s"${quote(name)}:[${rows.map(r => quote(r.toString)).mkString(",")}]"
      }.mkString(",")
      s"""{"id":${c.id},"path":${quote(c.path)},"verified":${c.verified},""" +
        s""""note":${quote(c.note)},"tables":{$tables}}"""
    }.mkString(",")

    s"""{"totalBranches":$totalBranches,"coverage":$coverage,"cases":[$casesJson]}"""
  }
}

/**
 * Systematic test-input generation for Spark SQL.
 *
 * Fuzzing searches for inputs by mutating what it has. Symbolic test generation works
 * the other way round: it reads the query's own conditions, solves them, and produces
 * one input per path through them. Where fuzzing eventually stumbles onto a branch,
 * this constructs a record that takes it.
 *
 * This is BigTest's approach (ESEC/FSE 2019), applied to SQL predicates. With
 * `natural = true` it is NaturalSym's (FSE 2024): the same paths, but with witnesses
 * drawn from values that really occur, so the generated tests read like data.
 *
 * Obtain an instance from [[BigAsterisk.testgen]].
 *
 * {{{
 * val suite = BigAsterisk.testgen(spark).generate(
 *   "SELECT cid FROM orders WHERE amount > 100",
 *   Map("orders" -> orders))
 *
 * suite.coverage
 * suite.cases.foreach(println)
 * }}}
 *
 * @group testgen
 */
trait TestGenSupport {

  /**
   * Generates a test suite for `query`.
   *
   * @param seeds the tables the query reads, by the name it reads them under. Schemas
   *        are required; rows are used as the pool of natural witnesses.
   */
  def generate(query: String, seeds: Map[String, DataFrame], config: TestGenConfig): TestSuite

  /** Generates a suite with the default configuration. */
  final def generate(query: String, seeds: Map[String, DataFrame]): TestSuite =
    generate(query, seeds, TestGenConfig())

  /**
   * [[generate]] for callers that cannot build a Scala `Map` or a [[TestGenConfig]] —
   * Java, and Py4J, which marshals a Python dict to `java.util.Map`.
   */
  final def generateJava(
      query: String,
      seeds: java.util.Map[String, DataFrame],
      maxPaths: Int,
      rowsPerPath: Int,
      natural: Boolean,
      seed: Long): TestSuite = {
    import scala.jdk.CollectionConverters._
    generate(query, seeds.asScala.toMap,
      TestGenConfig(maxPaths, rowsPerPath, natural, seed))
  }
}
