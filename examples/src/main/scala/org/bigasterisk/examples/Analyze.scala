package org.bigasterisk.examples

import scala.util.control.NonFatal

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.bigasterisk.api._
import org.bigasterisk.optdebug.OptDebug

/**
 * Run the tools against *your* query and *your* data.
 *
 * ==Why this exists==
 * The tour and the benchmarks answer "does it work?" on inputs chosen in advance. This
 * answers the question anyone actually has: point a tool at a job of mine. Nothing here
 * knows anything about the demo datasets — tables are whatever files you name, the query
 * is whatever SQL you pass, and the oracle is your own predicate over the result.
 *
 * {{{
 * bin/bigasterisk analyze \
 *   --table orders=examples/data/orders.txt \
 *   --schema orders="oid STRING, cid STRING, amount INT" \
 *   --query "SELECT cid, SUM(amount) AS total FROM orders GROUP BY cid" \
 *   --tool desql
 *
 * # a tool that needs to know which rows are wrong takes an oracle
 * bin/bigasterisk analyze ... --tool optdebug --oracle "total < 0"
 * }}}
 *
 * Every tool is available; those that need a fault model say so rather than inventing
 * one.
 */
object Analyze {

  private val Tools = Seq(
    "desql", "titian", "flowdebug", "bigsift", "optdebug", "perfdebug",
    "watchpoint", "crash", "vega", "bigtest", "fuzz", "all")

  /** How the run was configured. */
  private case class Options(
      tables: Map[String, String] = Map.empty,
      schemas: Map[String, String] = Map.empty,
      formats: Map[String, String] = Map.empty,
      query: Option[String] = None,
      oracle: Option[String] = None,
      watch: Option[String] = None,
      tool: String = "desql",
      limit: Int = 20,
      master: Option[String] = None)

  def main(args: Array[String]): Unit = {
    val code = execute(args)
    if (code != 0) System.exit(code)
  }

  /**
   * The whole run, as an exit code rather than a call to `System.exit`.
   *
   * Separated so it can be driven from a test: exiting the JVM from inside the work
   * takes the test runner with it.
   */
  private[examples] def execute(args: Array[String]): Int = {
    val options =
      try parse(args.toList, Options())
      catch {
        case e: IllegalArgumentException =>
          println(s"${e.getMessage}\n")
          usage()
          return 2
      }

    if (options.tables.isEmpty || options.query.isEmpty) {
      println("Both --table and --query are required.\n")
      usage()
      return 2
    }
    if (options.tool == "help") return 0
    if (!Tools.contains(options.tool)) {
      println(s"Unknown --tool '${options.tool}'. Known: ${Tools.mkString(", ")}")
      return 2
    }

    val builder = SparkSession.builder()
      .appName(s"BigAsterisk analyze (${options.tool})")
      .config("spark.sql.shuffle.partitions", "8")

    // A master given here wins; otherwise whatever spark-submit supplied; otherwise local.
    options.master.foreach(builder.master)
    if (options.master.isEmpty && !new org.apache.spark.SparkConf().contains("spark.master")) {
      builder.master("local[*]")
    }

    val spark = BigAsterisk.configure(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try {
      val tables = register(spark, options)
      println(s"\n${tables.size} table(s) registered; running ${options.tool}\n")

      val query = options.query.get
      val chosen = if (options.tool == "all") Tools.filterNot(_ == "all") else Seq(options.tool)
      var failed = 0

      chosen.foreach { tool =>
        println(s"── $tool")
        try run(spark, tool, query, tables, options)
        catch {
          case NonFatal(e) =>
            failed += 1
            println(s"  FAILED: ${e.getClass.getSimpleName}: " +
              Option(e.getMessage).map(_.split("\n").head).getOrElse(""))
        }
        println()
      }

      if (failed > 0) 1 else 0
    } finally {
      spark.stop()
    }
  }

  // ---------------------------------------------------------------------------

  /** Reads each `--table name=path` and registers it under its name. */
  private def register(spark: SparkSession, options: Options): Map[String, DataFrame] =
    options.tables.map { case (name, path) =>
      val format = options.formats.getOrElse(name, inferFormat(path))
      val reader = spark.read.format(format)
      val withSchema = options.schemas.get(name).fold(reader) { ddl =>
        reader.schema(ddl)
      }
      // a header is the usual case for a csv without a declared schema
      val configured =
        if (format == "csv" && !options.schemas.contains(name))
          withSchema.option("header", "true").option("inferSchema", "true")
        else withSchema

      val df = configured.load(path)
      df.createOrReplaceTempView(name)
      println(f"  $name%-16s $format%-8s ${df.count()}%,d rows  $path")
      name -> df
    }

  private def inferFormat(path: String): String = {
    val lower = path.toLowerCase
    if (lower.endsWith(".parquet") || lower.contains(".parquet")) "parquet"
    else if (lower.endsWith(".json")) "json"
    else if (lower.endsWith(".orc")) "orc"
    else "csv"
  }

  private def run(
      spark: SparkSession,
      tool: String,
      query: String,
      tables: Map[String, DataFrame],
      options: Options): Unit = tool match {

    case "desql" =>
      BigAsterisk.desql(spark).decompose(spark.sql(query)).foreach { step =>
        println(f"  [${step.id}%d] ${step.operator}%-12s ${step.detail.take(70)}")
        step.branches.foreach(b => println(s"        branch: ${b.description.take(66)}"))
      }

    case "titian" =>
      val lineage = BigAsterisk.lineage(spark)
      lineage.enableCapture(spark)
      try {
        val df = spark.sql(query)
        val outputs = lineage.collectWithLineage(df)
        println(s"  ${outputs.length} output row(s) captured")
        outputs.headOption.foreach { case (row, id) =>
          var cursor = lineage.trace(df, Seq(id))
          while (!cursor.atScan) cursor = cursor.goBack()
          println(s"  $row")
          println(s"    <- ${cursor.show().length} source record(s)")
        }
      } finally lineage.disableCapture(spark)

    case "flowdebug" =>
      val oracle = required(options.oracle, "flowdebug", "--oracle")
      BigAsterisk.influence(spark).influencers(spark.sql(query), oracle, options.limit)
        .foreach(i => println(s"  $i"))

    case "bigsift" =>
      val oracle = required(options.oracle, "bigsift", "--oracle")
      val base = baseTable(tables, options)
      val test = compileOracle(spark, query, oracle)
      val result = org.apache.spark.sql.bigsift.BigSiftSQL.debug(spark, base, query, test)
      println(s"  provenance left ${result.provenanceSize} candidate record(s)")
      println(s"  delta debugging narrowed them to ${result.faultInducingRows.size}")
      result.faultInducingRows.take(options.limit).foreach(r => println(s"    $r"))

    case "optdebug" =>
      val oracle = required(options.oracle, "optdebug", "--oracle")
      // With a single table the failing input is narrowed before scoring, which is what
      // makes a branch stand out from the operators that every record passes through.
      val result =
        if (tables.size == 1)
          OptDebug.localize(spark, tables.keys.head, query, compileOracle(spark, query, oracle))
        else OptDebug.localize(spark, spark.sql(query), oracle)
      result.ranked.take(options.limit).foreach(op => println(s"  $op"))

    case "perfdebug" =>
      val base = baseTable(tables, options)
      val profile = BigAsterisk.perfdebug(spark).profile(tables(base).coalesce(1), options.limit)
      profile.df.createOrReplaceTempView(base)
      spark.sql(query).collect()
      println(f"  ${profile.records} record(s), skew ${profile.skew}%.1fx the mean")
      profile.slowest.take(5).foreach(c => println(s"    $c"))
      tables(base).createOrReplaceTempView(base)

    case "watchpoint" =>
      val condition = required(options.watch, "watchpoint", "--watch")
      val base = baseTable(tables, options)
      val watched = BigAsterisk.watchpoints(spark)
        .watch(tables(base), org.apache.spark.sql.functions.expr(condition))
      watched.df.createOrReplaceTempView(base)
      spark.sql(query).collect()
      println(s"  ${watched.hits} record(s) matched $condition")
      watched.captured.take(options.limit).foreach(r => println(s"    $r"))
      tables(base).createOrReplaceTempView(base)

    case "crash" =>
      val base = baseTable(tables, options)
      val guard = BigAsterisk.crashCulprit(spark).guard(tables(base).coalesce(1))
      guard.df.createOrReplaceTempView(base)
      try {
        spark.sql(query).collect()
        println("  the query completed; nothing to attribute")
      } catch {
        case NonFatal(e) =>
          guard.culprit match {
            case Some(c) => println(s"  $c")
            case None    => println(s"  the query failed but no record was captured: ${e.getMessage}")
          }
      } finally tables(base).createOrReplaceTempView(base)

    case "vega" =>
      val incremental = BigAsterisk.vega(spark)
      try {
        incremental.run(spark.sql(query)).df.collect()
        val again = incremental.run(spark.sql(query))
        again.df.collect()
        println(f"  a second run reused ${again.reused.size} of ${again.steps} part(s) " +
          f"(${again.reuseRatio * 100}%.0f%%)")
        again.reused.foreach(part => println(s"    $part"))
        println("  (pass a revised --query to see what an edit can reuse)")
      } finally incremental.clear()

    case "bigtest" =>
      BigAsterisk.testgen(spark)
        .generate(query, tables, TestGenConfig(rowsPerPath = 1))
        .cases.take(options.limit).foreach(c => println(s"  $c"))

    case "fuzz" =>
      val result = BigAsterisk.fuzz(spark).fuzz(query, tables, FuzzConfig(iterations = 30))
      println(s"  $result")
      result.failures.take(options.limit).foreach(f => println(s"    $f"))

    case other =>
      throw new IllegalArgumentException(s"unknown tool '$other'")
  }

  /**
   * The table a record-level tool works over.
   *
   * With one table there is no ambiguity. With several, the caller has to say which, and
   * being told that is better than having one picked silently.
   */
  private def baseTable(tables: Map[String, DataFrame], options: Options): String =
    if (tables.size == 1) tables.keys.head
    else options.tables.keys.find(_ => false).getOrElse(
      throw new IllegalArgumentException(
        s"this tool works over one table, and ${tables.size} were given " +
        s"(${tables.keys.toSeq.sorted.mkString(", ")}). Pass just the one it should " +
        s"read, or use --tool desql to see the whole query."))

  /**
   * An oracle written as SQL over the query's output, compiled to a row predicate.
   *
   * Compiled, not memoised: input isolation re-runs the query over subsets, and every
   * re-run produces different aggregate values. An oracle that remembered which rows
   * were wrong the first time would call every later result correct, and delta debugging
   * would narrow nothing.
   */
  private def compileOracle(
      spark: SparkSession,
      query: String,
      predicate: String): org.apache.spark.sql.Row => Boolean =
    org.apache.spark.sql.oracle.SqlOracle.compile(spark, query, predicate)

  private def required[A](value: Option[A], tool: String, flag: String): A =
    value.getOrElse(throw new IllegalArgumentException(
      s"$tool needs $flag — it has to know which results are wrong"))

  // ---------------------------------------------------------------------------

  private def parse(args: List[String], options: Options): Options = args match {
    case Nil => options
    case "--table" :: value :: rest =>
      val (name, path) = split(value, "--table")
      parse(rest, options.copy(tables = options.tables + (name -> path)))
    case "--schema" :: value :: rest =>
      val (name, ddl) = split(value, "--schema")
      parse(rest, options.copy(schemas = options.schemas + (name -> ddl)))
    case "--format" :: value :: rest =>
      val (name, format) = split(value, "--format")
      parse(rest, options.copy(formats = options.formats + (name -> format)))
    case "--query" :: value :: rest    => parse(rest, options.copy(query = Some(value)))
    case "--query-file" :: value :: rest =>
      val text = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(value)))
      parse(rest, options.copy(query = Some(text)))
    case "--oracle" :: value :: rest   => parse(rest, options.copy(oracle = Some(value)))
    case "--watch" :: value :: rest    => parse(rest, options.copy(watch = Some(value)))
    case "--tool" :: value :: rest     => parse(rest, options.copy(tool = value.toLowerCase))
    case "--limit" :: value :: rest    => parse(rest, options.copy(limit = value.toInt))
    case "--master" :: value :: rest   => parse(rest, options.copy(master = Some(value)))
    case "--help" :: _ | "-h" :: _     => usage(); options.copy(tool = "help")
    case other :: _ =>
      throw new IllegalArgumentException(s"unrecognised argument: $other")
  }

  private def split(value: String, flag: String): (String, String) = {
    val at = value.indexOf('=')
    if (at <= 0) throw new IllegalArgumentException(s"$flag expects name=value, got '$value'")
    (value.substring(0, at), value.substring(at + 1))
  }

  private def usage(): Unit = println(
    """Run BigAsterisk's tools against your own query and data.
      |
      |  --table  name=path      a table to read; repeat for several
      |  --schema name=ddl       its schema, e.g. "a STRING, b INT" (else inferred)
      |  --format name=format    csv | parquet | json | orc (else inferred from the path)
      |  --query  "SELECT ..."   the query to analyse
      |  --query-file path       the query, read from a file
      |  --oracle "predicate"    SQL over the query's OUTPUT that is true for a WRONG row
      |  --watch  "predicate"    SQL over the input, for --tool watchpoint
      |  --tool   name           desql titian flowdebug bigsift optdebug perfdebug
      |                          watchpoint crash vega bigtest fuzz all   (default desql)
      |  --limit  n              rows to print per tool (default 20)
      |  --master url            override the Spark master
      |
      |Which tools need an oracle: flowdebug, bigsift, optdebug. They have to know which
      |results are wrong, and only you can say.
      |
      |Example:
      |  bin/bigasterisk analyze \
      |    --table orders=examples/data/orders.txt \
      |    --schema orders="oid STRING, cid STRING, amount INT" \
      |    --query "SELECT cid, SUM(amount) AS total FROM orders GROUP BY cid" \
      |    --tool desql
      |""".stripMargin)
}
