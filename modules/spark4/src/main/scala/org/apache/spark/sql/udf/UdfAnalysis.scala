package org.apache.spark.sql.udf

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualTo, Expression, Literal, PythonUDF, ScalaUDF}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}
import org.apache.spark.sql.types.BooleanType

import org.bigasterisk.api.{UdfProfile, UdfRegistry}

/**
 * Seeing inside a user-defined function.
 *
 * ==The boundary this crosses==
 * Plan analysis stops at a UDF. `WHERE classify(amount) = 'high'` is, to Catalyst, one
 * opaque call over one column: no branches to score, no constraints to solve, and every
 * argument equally implicated in the result. That is why operation-level fault
 * localisation, symbolic test generation and taint-refined provenance each stop there.
 *
 * A [[UdfProfile]] carries what static analysis of the function's own source found —
 * its branches, its paths, and which parameters actually influence what it returns.
 * This binds that back to the query: a parameter becomes the argument expression the
 * call site passes, so a condition written over `amount` becomes a condition over the
 * column the query passes as `amount`, and everything downstream treats it as an
 * ordinary predicate.
 *
 * ==Nothing happens without a profile==
 * A function whose insides could not be read is the black box it always was, so this can
 * only add information to a result, never change one that was already being produced.
 *
 * ==Where profiles come from==
 * The analysis has to happen where the code is, and the two languages differ:
 *
 *   - **Python** — the front end parses the function's source and registers what it
 *     found ([[UdfRegistry]]). Nothing is analysed on this side, because the code is not
 *     on this side.
 *   - **Scala and Java** — the closure's bytecode is on the classloader, and
 *     [[ScalaUdfAnalysis]] abstractly interprets it. Nothing has to be registered.
 *
 * A registered profile wins over a derived one: a user who described a function
 * explicitly knows more about it than its bytecode reveals.
 */
object UdfAnalysis {

  /**
   * A user-defined function call whose insides are known.
   *
   * The two languages arrive by different routes — a Python profile is registered from
   * the front end, a Scala one is derived from bytecode — but a call is a call, and
   * everything downstream treats them identically.
   */
  case class Call(arguments: Seq[Expression], profile: UdfProfile)

  /** Every profiled UDF call in `plan`'s own expressions. */
  def profiled(plan: LogicalPlan): Seq[Call] = plan.expressions.flatMap(profiled)

  /** Every profiled UDF call within `expression`. */
  def profiled(expression: Expression): Seq[Call] =
    expression.collect {
      case udf: PythonUDF => callOf(udf)
      case udf: ScalaUDF  => callOf(udf)
    }.flatten

  /** The call for one UDF expression, if its insides are known. */
  private def callOf(expression: Expression): Option[Call] = expression match {
    case udf: PythonUDF =>
      UdfRegistry.lookup(udf.name)
        .filter(profile => profile.parameters.size == udf.children.size &&
          udf.evalType == BatchedPythonUdf)
        .map(Call(udf.children, _))

    case udf: ScalaUDF =>
      // A registered profile wins: a user who described the function explicitly knows
      // more about it than its bytecode reveals.
      UdfRegistry.lookup(ScalaUdfAnalysis.nameOf(udf))
        .orElse(ScalaUdfAnalysis.profile(udf))
        .filter(_.parameters.size == udf.children.size)
        .map(Call(udf.children, _))

    case _ => None
  }


  /**
   * The functions whose *result* `condition` tests against a literal.
   *
   * A caller that has already rewritten such a test into conditions on the function's
   * arguments has covered that function's paths, and adding its branches again would
   * cross the two sets into combinations that contradict each other — a path enumerator
   * would then spend its budget on conjunctions no input can satisfy.
   */
  def testedFunctions(condition: Expression): Set[String] =
    condition.collect {
      case EqualTo(u: PythonUDF, _: Literal) => u.name
      case EqualTo(_: Literal, u: PythonUDF) => u.name
      case u: PythonUDF if u.dataType == BooleanType => u.name
      case EqualTo(u: ScalaUDF, _: Literal) => ScalaUdfAnalysis.nameOf(u)
      case EqualTo(_: Literal, u: ScalaUDF) => ScalaUdfAnalysis.nameOf(u)
      case u: ScalaUDF if u.dataType == BooleanType => ScalaUdfAnalysis.nameOf(u)
    }.toSet

  /**
   * The branch conditions inside `plan`'s user-defined functions, as predicates over the
   * columns the call site passes.
   *
   * These are ordinary conditions once bound: they can be evaluated against the step's
   * input rows, which is what makes a branch inside a UDF observable at record level —
   * and therefore scoreable, coverable and solvable like any other.
   *
   * A condition that cannot be bound and resolved is dropped rather than approximated.
   */
  def internalBranches(
      plan: LogicalPlan,
      spark: ClassicSparkSession,
      exclude: Set[String] = Set.empty): Seq[Expression] = {
    if (plan.children.size != 1) return Nil
    val child = plan.children.head

    profiled(plan)
      .filterNot(call => exclude.contains(call.profile.name))
      .flatMap { call =>
        call.profile.branches.flatMap(branch => bind(branch.condition, call, child, spark))
      }
      .distinct
  }

  /**
   * Rewrites a condition that tests a UDF's *result* into conditions on its *inputs*,
   * one per path that produces that result.
   *
   * `classify(amount) = 'high'` is unsolvable as written — a solver cannot invert an
   * opaque call. Given a profile whose paths are exact and exhaustive, it is equivalent
   * to the conditions under which `classify` returns `'high'`, and those are ordinary
   * comparisons on `amount`.
   *
   * Returns `None` when the condition is not of this shape, or when the profile is not
   * exact enough to stand in for it — in which case the caller keeps the original and
   * reports the path as unsupported, which is the honest outcome.
   */
  def solveThrough(
      condition: Expression,
      child: LogicalPlan,
      spark: ClassicSparkSession): Option[Seq[Expression]] = {
    val (udf, wanted) = condition match {
      case EqualTo(u: PythonUDF, l: Literal) => (u: Expression, l)
      case EqualTo(l: Literal, u: PythonUDF) => (u: Expression, l)
      case EqualTo(u: ScalaUDF, l: Literal)  => (u: Expression, l)
      case EqualTo(l: Literal, u: ScalaUDF)  => (u: Expression, l)
      // `WHERE flag(x)` — a boolean UDF used as a predicate on its own
      case u: PythonUDF if u.dataType == BooleanType => (u: Expression, Literal(true))
      case u: ScalaUDF if u.dataType == BooleanType  => (u: Expression, Literal(true))
      case _                                         => return None
    }

    val call = callOf(udf).getOrElse(return None)
    if (!call.profile.isSolvable) return None

    val target = render(wanted)
    val yielding =
      call.profile.paths.filter(p => p.exact && p.returns.exists(matches(_, target)))
    if (yielding.isEmpty) return None

    val bound = yielding.flatMap(p => bind(p.constraint, call, child, spark))
    if (bound.size == yielding.size) Some(bound.distinct) else None
  }

  /**
   * The attributes whose value can change the value of `expression`.
   *
   * Ordinarily that is every attribute it references. Where a profiled UDF is
   * involved it is fewer: an argument the function never lets reach its result — or one
   * that only decides a branch whose arms return the same thing — provably cannot
   * change the outcome, however prominently it appears in the call.
   *
   * That refinement is the point of taint analysis: without it, provenance implicates
   * every column the call mentions.
   */
  def influencing(expression: Expression): Set[Attribute] = expression match {
    case _: PythonUDF | _: ScalaUDF =>
      callOf(expression) match {
        case Some(call) =>
          call.arguments.zipWithIndex
            .filter { case (_, index) => call.profile.influences(index) }
            .flatMap { case (argument, _) => influencing(argument) }
            .toSet
        case None => expression.references.toSet
      }
    case _ if expression.children.isEmpty => expression.references.toSet
    case _                                => expression.children.flatMap(influencing).toSet
  }

  /**
   * Binds `text` — a condition over the function's parameter names — to the call site.
   *
   * Each parameter is replaced by the argument expression passed for it, and the result
   * is resolved against `child` by Spark's own analyzer, which is also what checks that
   * it is a well-typed boolean over columns that exist. Anything that fails to parse,
   * bind or resolve yields `None`.
   */
  private def bind(
      text: String,
      call: Call,
      child: LogicalPlan,
      spark: ClassicSparkSession): Option[Expression] = {
    // Spark wraps a UDF argument in KnownNotNull when the function takes a primitive.
    // It is a no-op for a predicate, and leaving it in would spell the same branch
    // differently from the query's own conditions.
    val arguments = call.profile.parameters.zip(call.arguments.map {
      case org.apache.spark.sql.catalyst.expressions.KnownNotNull(inner) => inner
      case other                                                        => other
    }).toMap
    try {
      val parsed = CatalystSqlParser.parseExpression(text)
      var unknown = false
      val substituted = parsed.transformUp {
        case attribute: UnresolvedAttribute =>
          arguments.get(attribute.name) match {
            case Some(argument) => argument
            case None           => unknown = true; attribute
          }
      }
      if (unknown) None else resolve(substituted, child, spark)
    } catch {
      case NonFatal(_) => None
    }
  }

  /** Resolves `condition` against `child`, or `None` if it is not a valid predicate. */
  private def resolve(
      condition: Expression,
      child: LogicalPlan,
      spark: ClassicSparkSession): Option[Expression] =
    try {
      val analyzer = spark.sessionState.analyzer
      val analyzed = analyzer.execute(Filter(condition, child))
      analyzer.checkAnalysis(analyzed)
      analyzed match {
        case f: Filter if f.condition.dataType == BooleanType && f.condition.deterministic =>
          Some(f.condition)
        case _ => None
      }
    } catch {
      case NonFatal(_) => None
    }

  /** `PythonEvalType.SQL_BATCHED_UDF`: a plain, row-at-a-time Python UDF. */
  private[udf] val BatchedPythonUdf = 100

  /** A literal as comparable text, so `'high'` from Python matches Catalyst's own. */
  private def render(literal: Literal): String =
    if (literal.value == null) "null" else String.valueOf(literal.value)

  /** Whether the SQL text of a profile's return value denotes `target`. */
  private def matches(returns: String, target: String): Boolean =
    try {
      val parsed = CatalystSqlParser.parseExpression(returns)
      if (!parsed.foldable) false
      else {
        val value = parsed.eval(null)
        val text = if (value == null) "null" else String.valueOf(value)
        text == target
      }
    } catch {
      case NonFatal(_) => false
    }
}
