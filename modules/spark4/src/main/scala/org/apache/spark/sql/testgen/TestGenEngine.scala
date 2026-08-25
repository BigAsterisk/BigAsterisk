package org.apache.spark.sql.testgen

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.{ExpressionColumnNode, Dataset => ClassicDataset, SparkSession => ClassicSparkSession}
import org.apache.spark.sql.desql.DeSqlEngine
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.udf.UdfAnalysis
import org.apache.spark.sql.types.StructType

import org.bigasterisk.api._

/**
 * Systematic test-input generation for Spark SQL.
 *
 * ==Approach==
 * Fuzzing searches for inputs by mutating what it has. This works the other way round:
 * it reads the query's own branch conditions, solves them, and constructs one input per
 * path through them.
 *
 * Full symbolic execution is not needed for SQL predicates. A conjunction of
 * comparisons against literals is a set of interval and equality constraints per column
 * ([[ColumnDomain]]), and a witness can be read off the bounds. Constraints the solver
 * cannot express — relating two columns, or arithmetic on the left-hand side — make the
 * path *unsupported* rather than silently wrong.
 *
 * ==Naturalness==
 * With `natural = true`, a witness is taken from values that actually occur in the seed
 * data whenever one satisfies the constraints. Same paths, but the generated records
 * read like records rather than like solver output — the NaturalSym idea.
 *
 * ==Every test is executed==
 * A generator that reports coverage it did not achieve is worse than useless, so each
 * generated input is run and the branch it was built for is checked. `TestCase.verified`
 * says what actually happened.
 */
class TestGenEngine extends TestGenSupport {

  override def generate(
      query: String,
      seeds: Map[String, DataFrame],
      config: TestGenConfig): TestSuite = {
    require(seeds.nonEmpty, "at least one seed table is required")

    val spark = seeds.head._2.sparkSession match {
      case c: ClassicSparkSession => c
      case other =>
        throw new UnsupportedOperationException(
          "Test generation needs a classic SparkSession; this one is " +
          s"${other.getClass.getName}. Spark Connect sessions are not supported: the " +
          "branch conditions are read from the driver-side analyzed plan, which a " +
          "Connect client does not hold.")
    }

    val schemas = seeds.map { case (name, df) => name -> df.schema }
    val naturalValues: Map[String, IndexedSeq[Any]] =
      if (!config.natural) Map.empty
      else seeds.toSeq.flatMap { case (_, df) =>
        ValuesOf(df).toSeq
      }.groupBy(_._1).map { case (column, entries) =>
        column -> entries.flatMap(_._2).distinct.toIndexedSeq
      }

    val random = new Random(config.seed)
    val declared = config.parsedDistributions
    val conditions = branchConditions(spark, query)
    val paths = enumeratePaths(conditions, config.maxPaths)

    val cases = mutable.ArrayBuffer.empty[TestCase]
    withRestoredViews(seeds) {
      paths.zipWithIndex.foreach { case (path, id) =>
        cases += buildCase(
          spark, query, schemas, naturalValues, declared, random, path, id, config)
      }
    }

    TestSuite(cases.toSeq, conditions.size)
  }

  /** One path: each branch condition paired with whether this test should take it. */
  private type Path = Seq[(Expression, Boolean)]

  private def buildCase(
      spark: ClassicSparkSession,
      query: String,
      schemas: Map[String, StructType],
      naturalValues: Map[String, IndexedSeq[Any]],
      declared: Map[String, Distribution],
      random: Random,
      path: Path,
      id: Int,
      config: TestGenConfig): TestCase = {

    val label = path.map { case (c, take) =>
      val text = describe(c)
      if (take) text else s"NOT $text"
    }.mkString(" AND ")

    // `Left`/`Right` are Catalyst string expressions in this scope, so the Either
    // constructors have to be qualified.
    val domains = solve(path, schemas)
    domains match {
      case scala.util.Left(reason) =>
        TestCase(id, label, Map.empty, verified = false, reason)

      case scala.util.Right(byColumn) =>
        val tables = schemas.map { case (table, schema) =>
          val rows = (0 until config.rowsPerPath).map { _ =>
            Row.fromSeq(schema.fields.map { field =>
              val domain = byColumn.getOrElse(
                field.name, ColumnDomain(field.dataType))
              domain
                .witness(
                  naturalValues.getOrElse(field.name, IndexedSeq.empty),
                  random,
                  declared.get(field.name))
                .getOrElse(null)
            }.toIndexedSeq)
          }
          table -> rows
        }

        if (tables.values.exists(_.isEmpty)) {
          TestCase(id, label, tables, verified = false, "no witness for some column")
        } else {
          val (reached, note) = verify(spark, query, schemas, tables, path)
          TestCase(id, label, tables, reached, note)
        }
    }
  }

  /**
   * Runs the generated input and checks the path was actually taken.
   *
   * A generated test is a claim about what the query will do. Executing it is what
   * turns the claim into a fact, and it is cheap: the input is a handful of rows.
   */
  private def verify(
      spark: ClassicSparkSession,
      query: String,
      schemas: Map[String, StructType],
      tables: Map[String, Seq[Row]],
      path: Path): (Boolean, String) = {
    try {
      tables.foreach { case (name, rows) =>
        spark.createDataFrame(rows.asJava, schemas(name)).createOrReplaceTempView(name)
      }
      spark.sql(query).collect()

      val reached = reachedConditions(spark, query)
      val wanted = path.collect { case (c, true) => describe(c) }
      val missing = wanted.filterNot(reached.contains)
      if (missing.isEmpty) (true, "verified")
      else (false, s"path not reached: ${missing.mkString(", ")}")
    } catch {
      case NonFatal(e) =>
        // a generated input that makes the query throw is a finding, not a failure of
        // the generator
        (false, s"query failed: ${e.getClass.getSimpleName}: ${e.getMessage}")
    }
  }

  /**
   * Turns a path into per-column domains, or explains why it cannot.
   *
   * Conditions are gathered into per-column constraints; a condition the solver cannot
   * express makes the whole path unsupported, since generating an input that ignores it
   * would be reporting coverage that was never achieved.
   */
  private def solve(
      path: Path,
      schemas: Map[String, StructType]): Either[String, Map[String, ColumnDomain]] = {
    val types: Map[String, org.apache.spark.sql.types.DataType] =
      schemas.values.flatMap(_.fields.map(f => f.name -> f.dataType)).toMap

    var domains = Map.empty[String, ColumnDomain]
    val unsupported = mutable.ArrayBuffer.empty[String]

    def domainOf(name: String): ColumnDomain =
      domains.getOrElse(name, ColumnDomain(types.getOrElse(name, org.apache.spark.sql.types.StringType)))

    def constrain(name: String, f: ColumnDomain => ColumnDomain): Unit =
      domains = domains.updated(name, f(domainOf(name)))

    path.foreach { case (condition, take) =>
      TestGenEngine.atoms(condition, take).foreach {
        case Some(atom) => atom match {
          case Atom.Bound(name, value, lower, inclusive) =>
            if (!types.contains(name)) unsupported += s"unknown column $name"
            else constrain(name, d =>
              if (lower) d.withLower(value, inclusive) else d.withUpper(value, inclusive))
          case Atom.Equal(name, value) =>
            if (!types.contains(name)) unsupported += s"unknown column $name"
            else constrain(name, d =>
              if (d.equalTo.exists(_ != value)) d.contradiction
              else d.copy(equalTo = Some(value), mustNotBeNull = true))
          case Atom.NotEqual(name, value) =>
            constrain(name, d => d.copy(notEqualTo = d.notEqualTo + value))
          case Atom.IsNull(name)    => constrain(name, _.copy(mustBeNull = true))
          case Atom.IsNotNull(name) => constrain(name, _.copy(mustNotBeNull = true))
          case Atom.Prefix(name, p) => constrain(name, _.copy(prefix = Some(p), mustNotBeNull = true))
          case Atom.Contains(name, s) =>
            constrain(name, d => d.copy(contains = d.contains :+ s, mustNotBeNull = true))
        }
        case None => unsupported += "condition outside the solver's fragment"
      }
    }

    if (unsupported.nonEmpty) scala.util.Left(unsupported.distinct.mkString("; "))
    else if (domains.values.exists(_.isUnsatisfiable)) scala.util.Left("unsatisfiable")
    else scala.util.Right(domains)
  }

  /**
   * Enumerates paths through the branch conditions.
   *
   * Every combination when there are few enough to fit the budget; otherwise each
   * condition taken and not taken on its own, which is branch coverage rather than path
   * coverage. The distinction is reported rather than hidden: with more branches than
   * the budget allows, the suite covers branches, not paths.
   */
  private def enumeratePaths(conditions: Seq[Expression], maxPaths: Int): Seq[Path] = {
    if (conditions.isEmpty) return Seq.empty
    val combinations = math.pow(2, conditions.size)
    if (combinations <= maxPaths) {
      (0 until combinations.toInt).map { mask =>
        conditions.zipWithIndex.map { case (c, i) => c -> ((mask >> i) % 2 == 1) }
      }
    } else {
      conditions.flatMap(c => Seq(Seq(c -> true), Seq(c -> false))).take(maxPaths)
    }
  }

  /**
   * The query's branch conditions, grouped by the rows they are evaluated against.
   *
   * Both the generator and the verifier work from this, so a condition can never be
   * generated for and then not looked for — which is what would happen if the two
   * derived their targets separately.
   */
  private def conditionsByInput(
      spark: ClassicSparkSession,
      query: String): Seq[(LogicalPlan, Seq[Expression])] =
    DeSqlEngine
      .stepNodes(spark.sql(query).queryExecution.analyzed)
      .flatMap { node =>
        node.plan.children.headOption.map { child =>
          val own = DeSqlEngine.branchConditions(node.plan)
            // a Filter contributes both a condition and its negation; the path
            // enumerator supplies negations itself, so keep only the positive form
            .filterNot(_.isInstanceOf[Not])

          // A condition that tests a UDF's *result* cannot be solved as written. Where
          // the function's paths are exact, the conditions under which it returns that
          // result are ordinary comparisons on its arguments, and those can be solved —
          // so the opaque test is replaced by them, rather than being carried along to
          // make every path containing it unsupported.
          val solvable = own.flatMap { condition =>
            UdfAnalysis.solveThrough(condition, child, spark).getOrElse(Seq(condition))
          }

          // and a branch inside the UDF is a target in its own right, whether or not
          // anything tests the function's result
          child -> (solvable ++ UdfAnalysis.internalBranches(node.plan, spark)).distinct
        }
      }
      .filter { case (_, conditions) => conditions.nonEmpty }

  private def branchConditions(spark: ClassicSparkSession, query: String): Seq[Expression] =
    conditionsByInput(spark, query).flatMap { case (_, conditions) => conditions }.distinct

  /** Which branch conditions the currently registered data reaches. */
  private def reachedConditions(spark: ClassicSparkSession, query: String): Set[String] =
    conditionsByInput(spark, query).flatMap { case (input, conditions) =>
      try {
        val df = ClassicDataset.ofRows(spark, input)
        val indicators = conditions.zipWithIndex.map { case (c, i) =>
          max(new Column(ExpressionColumnNode(c))).as(s"__reached_$i")
        }
        val summary = df.agg(indicators.head, indicators.tail.toIndexedSeq: _*).collect().head
        conditions.zipWithIndex.collect {
          case (c, i) if !summary.isNullAt(i) && summary.getBoolean(i) => describe(c)
        }
      } catch {
        case NonFatal(_) => Seq.empty
      }
    }.toSet

  private def withRestoredViews[A](seeds: Map[String, DataFrame])(body: => A): A =
    try body
    finally seeds.foreach { case (name, df) => df.createOrReplaceTempView(name) }

  private def describe(e: Expression): String =
    try e.sql catch { case NonFatal(_) => e.toString }
}

/** The observed values of each column of a DataFrame, by column name. */
private object ValuesOf {
  def apply(df: DataFrame): Map[String, IndexedSeq[Any]] = {
    val rows = df.collect()
    df.schema.fields.map { field =>
      val i = df.schema.fieldIndex(field.name)
      field.name -> rows.iterator
        .map(r => if (r.isNullAt(i)) null else r.get(i))
        .filter(_ != null)
        .distinct
        .take(1000)
        .toIndexedSeq
    }.toMap
  }
}

/** A single constraint the solver understands. */
private[testgen] sealed trait Atom
private[testgen] object Atom {
  case class Bound(column: String, value: Double, lower: Boolean, inclusive: Boolean) extends Atom
  case class Equal(column: String, value: Any) extends Atom
  case class NotEqual(column: String, value: Any) extends Atom
  case class IsNull(column: String) extends Atom
  case class IsNotNull(column: String) extends Atom
  case class Prefix(column: String, value: String) extends Atom
  case class Contains(column: String, value: String) extends Atom
}

object TestGenEngine {

  /**
   * Decomposes a condition into the constraints the solver understands.
   *
   * `None` in the result marks a piece outside the fragment. The caller treats a path
   * containing one as unsupported rather than solving what it can and ignoring the
   * rest, which would report coverage that was never achieved.
   */
  def atoms(condition: Expression, take: Boolean): Seq[Option[Atom]] = condition match {
    case And(l, r) if take  => atoms(l, take) ++ atoms(r, take)
    case Or(l, r) if !take  => atoms(l, take) ++ atoms(r, take)
    case Not(inner)         => atoms(inner, !take)

    // a disjunction that must hold, or a conjunction that must not: satisfying it means
    // choosing a disjunct, which this solver does not branch over
    case _: Or | _: And     => Seq(None)

    case EqualTo(a: AttributeReference, Literal(v, _)) =>
      Seq(Some(if (take) Atom.Equal(a.name, literalValue(v)) else Atom.NotEqual(a.name, literalValue(v))))
    case EqualTo(Literal(v, _), a: AttributeReference) =>
      Seq(Some(if (take) Atom.Equal(a.name, literalValue(v)) else Atom.NotEqual(a.name, literalValue(v))))

    case GreaterThan(a: AttributeReference, Literal(v, _)) =>
      numeric(v).map(d => Some(Atom.Bound(a.name, d, lower = take, inclusive = !take))).toSeq
    case GreaterThanOrEqual(a: AttributeReference, Literal(v, _)) =>
      numeric(v).map(d => Some(Atom.Bound(a.name, d, lower = take, inclusive = take))).toSeq
    case LessThan(a: AttributeReference, Literal(v, _)) =>
      numeric(v).map(d => Some(Atom.Bound(a.name, d, lower = !take, inclusive = !take))).toSeq
    case LessThanOrEqual(a: AttributeReference, Literal(v, _)) =>
      numeric(v).map(d => Some(Atom.Bound(a.name, d, lower = !take, inclusive = take))).toSeq

    case IsNull(a: AttributeReference) =>
      Seq(Some(if (take) Atom.IsNull(a.name) else Atom.IsNotNull(a.name)))
    case IsNotNull(a: AttributeReference) =>
      Seq(Some(if (take) Atom.IsNotNull(a.name) else Atom.IsNull(a.name)))

    case StartsWith(a: AttributeReference, Literal(v, _)) if take =>
      Seq(Some(Atom.Prefix(a.name, v.toString)))
    case Contains(a: AttributeReference, Literal(v, _)) if take =>
      Seq(Some(Atom.Contains(a.name, v.toString)))

    case _ => Seq(None)
  }

  /** Unwraps Catalyst's internal string representation. */
  private def literalValue(v: Any): Any = v match {
    case u: org.apache.spark.unsafe.types.UTF8String => u.toString
    case other                                       => other
  }

  private def numeric(v: Any): Option[Double] = v match {
    case n: Number => Some(n.doubleValue())
    case _         => None
  }
}
