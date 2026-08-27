package org.bigasterisk.examples

import java.io.{ByteArrayOutputStream, File, PrintWriter}
import java.nio.file.Files

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * The general entry point, exercised on data it has never seen.
 *
 * The fixtures here are written by the test rather than taken from `examples/data`, for
 * the reason this entry point exists at all: nothing about it should be specific to the
 * datasets that happen to ship with the project.
 */
class AnalyzeSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var directory: File = _
  private var salesPath: String = _
  private var regionsPath: String = _

  override def beforeAll(): Unit = {
    directory = Files.createTempDirectory("analyze-suite").toFile

    // a table with a planted outlier, in a shape the project has no fixture for
    salesPath = new File(directory, "sales.csv").getAbsolutePath
    write(salesPath,
      "sid,region,revenue",
      "s1,north,120",
      "s2,north,90",
      "s3,south,150",
      "s4,south,60",
      "s5,east,999999")

    regionsPath = new File(directory, "regions.csv").getAbsolutePath
    write(regionsPath, "region,manager", "north,alice", "south,bob", "east,carol")
  }

  override def afterAll(): Unit = {
    def delete(f: File): Unit = {
      if (f.isDirectory) Option(f.listFiles).foreach(_.foreach(delete))
      f.delete()
    }
    if (directory != null) delete(directory)
  }

  private def write(path: String, lines: String*): Unit = {
    val writer = new PrintWriter(path)
    try lines.foreach(writer.println)
    finally writer.close()
  }

  /** Runs the CLI, returning everything it printed. */
  private def analyze(args: String*): String = run(args: _*)._2

  /** Runs the CLI, returning its exit code and everything it printed. */
  private def run(args: String*): (Int, String) = {
    val captured = new ByteArrayOutputStream()
    var code = 0
    Console.withOut(captured) {
      code = Analyze.execute(args.toArray ++ Array("--master", "local[2]"))
    }
    (code, captured.toString)
  }

  private val query =
    "SELECT region, SUM(revenue) AS total FROM sales GROUP BY region"

  test("a table is read and registered under the name it is given") {
    val output = analyze(
      "--table", s"sales=$salesPath", "--query", query, "--tool", "desql")

    output should include("sales")
    output should include("5 rows")
    output should include("Aggregate")
  }

  test("a schema can be declared rather than inferred") {
    val output = analyze(
      "--table", s"sales=$salesPath",
      "--schema", "sales=sid STRING, region STRING, revenue INT",
      "--query", "SELECT region, SUM(revenue) AS total FROM sales GROUP BY region",
      "--tool", "desql")

    output should include("Aggregate")
  }

  test("provenance runs on a table the project has never seen") {
    val output = analyze(
      "--table", s"sales=$salesPath", "--query", query, "--tool", "titian")

    output should include("output row(s) captured")
    output should include("source record(s)")
  }

  test("influence ranks the outlier behind a wrong total") {
    val output = analyze(
      "--table", s"sales=$salesPath", "--query", query,
      "--oracle", "total > 1000", "--tool", "flowdebug")

    output should include("999999")
  }

  test("input isolation narrows to the record that causes it") {
    val output = analyze(
      "--table", s"sales=$salesPath", "--query", query,
      "--oracle", "total > 1000", "--tool", "bigsift")

    output should include("narrowed them to 1")
    output should include("999999")
  }

  test("several tables can be joined") {
    val output = analyze(
      "--table", s"sales=$salesPath",
      "--table", s"regions=$regionsPath",
      "--query",
      "SELECT r.manager, SUM(s.revenue) AS total FROM sales s " +
        "JOIN regions r ON s.region = r.region GROUP BY r.manager",
      "--tool", "desql")

    output should include("Join")
    output should include("2 table(s) registered")
  }

  test("a tool that needs an oracle says so rather than inventing one") {
    val output = analyze(
      "--table", s"sales=$salesPath", "--query", query, "--tool", "flowdebug")

    output should include("--oracle")
  }

  test("test generation and fuzzing need no fault model at all") {
    val generated = analyze(
      "--table", s"sales=$salesPath",
      "--query", "SELECT sid FROM sales WHERE revenue > 100", "--tool", "bigtest")
    generated should include("revenue > 100")

    val fuzzed = analyze(
      "--table", s"sales=$salesPath",
      "--query", "SELECT sid FROM sales WHERE revenue > 100", "--tool", "fuzz")
    fuzzed should include("FuzzResult")
  }

  test("a query can be read from a file") {
    val queryFile = new File(directory, "query.sql").getAbsolutePath
    write(queryFile, query)

    val output = analyze(
      "--table", s"sales=$salesPath", "--query-file", queryFile, "--tool", "desql")
    output should include("Aggregate")
  }

  test("an unparseable argument is refused with a usage message, not a stack trace") {
    val (code, output) = run("--table", "missing-equals-sign", "--query", query)
    code shouldBe 2
    output should include("name=value")
    output should include("--tool")
  }

  test("a missing query is refused") {
    val (code, output) = run("--table", s"sales=$salesPath")
    code shouldBe 2
    output should include("required")
  }

  test("an unknown tool is refused, and the known ones are listed") {
    val (code, output) = run(
      "--table", s"sales=$salesPath", "--query", query, "--tool", "nonexistent")
    code shouldBe 2
    output should include("bigsift")
  }
}
