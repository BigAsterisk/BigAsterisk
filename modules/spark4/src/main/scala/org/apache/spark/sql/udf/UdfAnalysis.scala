package org.apache.spark.sql.udf

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualTo, Expression, Literal, PythonUDF}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}
import org.apache.spark.sql.types.BooleanType

import org.bigasterisk.api.{UdfProfile, UdfRegistry}

/**
 * Seeing inside a Python UDF.
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
 * The registry is empty until the Python front end fills it. A function with no profile
 * is the black box it always was, so this can only add information to a result, never
 * change one that was already being produced.
 *
 * ==Why only Python==
 * The analysis has to happen where the code is. A Python UDF's body is Python source,
 * which the Python front end can read; a Scala or Java UDF arrives here as a closure
 * whose logic is JVM bytecode, and reading that is a different analysis entirely.
 * Non-Python UDFs are left alone rather than guessed at.
 */
object UdfAnalysis {

  /** Every Python UDF in `plan`'s own expressions that has a registered profile. */
  def profiled(plan: LogicalPlan): Seq[(PythonUDF, UdfProfile)] =
    plan.expressions.flatMap(profiled)

  /** Every Python UDF within `expression` that has a registered profile. */
  def profiled(expression: Expression): Seq[(PythonUDF, UdfProfile)] =
    expression.collect { case udf: PythonUDF => udf }.flatMap { udf =>
      UdfRegistry.lookup(udf.name).filter(usable(udf, _)).map(udf -> _)
    }

  /**
   * The branch conditions inside `plan`'s Python UDFs, as predicates over the columns
   * the call site passes.
   *
   * These are ordinary conditions once bound: they can be evaluated against the step's
   * input rows, which is what makes a branch inside a UDF observable at record level —
   * and therefore scoreable, coverable and solvable like any other.
   *
   * A condition that cannot be bound and resolved is dropped rather than approximated.
   */
  def internalBranches(plan: LogicalPlan, spark: ClassicSparkSession): Seq[Expression] = {
    if (UdfRegistry.size == 0 || plan.children.size != 1) return Nil
    val child = plan.children.head

    profiled(plan).flatMap { case (udf, profile) =>
      profile.branches.flatMap { branch =>
        bind(branch.condition, udf, profile, child, spark)
      }
    }.distinct
  }

  /**
   * Rewrites a condition that tests a Python UDF's *result* into conditions on its
   * *inputs*, one per path that produces that result.
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
    if (UdfRegistry.size == 0) return None

    val (udf, wanted) = condition match {
      case EqualTo(u: PythonUDF, l: Literal) => (u, l)
      case EqualTo(l: Literal, u: PythonUDF) => (u, l)
      // `WHERE flag(x)` — a boolean UDF used as a predicate on its own
      case u: PythonUDF if u.dataType == BooleanType => (u, Literal(true))
      case _                                         => return None
    }

    val profile = UdfRegistry.lookup(udf.name).filter(usable(udf, _)).getOrElse(return None)
    if (!profile.isSolvable) return None

    val target = render(wanted)
    val yielding = profile.paths.filter(p => p.exact && p.returns.exists(matches(_, target)))
    if (yielding.isEmpty) return None

    val bound = yielding.flatMap(p => bind(p.constraint, udf, profile, child, spark))
    if (bound.size == yielding.size) Some(bound.distinct) else None
  }

  /**
   * The attributes whose value can change the value of `expression`.
   *
   * Ordinarily that is every attribute it references. Where a profiled Python UDF is
   * involved it is fewer: an argument the function never lets reach its result — or one
   * that only decides a branch whose arms return the same thing — provably cannot
   * change the outcome, however prominently it appears in the call.
   *
   * That refinement is the point of taint analysis: without it, provenance implicates
   * every column the call mentions.
   */
  def influencing(expression: Expression): Set[Attribute] = expression match {
    case udf: PythonUDF =>
      UdfRegistry.lookup(udf.name).filter(usable(udf, _)) match {
        case Some(profile) =>
          udf.children.zipWithIndex
            .filter { case (_, index) => profile.influences(index) }
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
      udf: PythonUDF,
      profile: UdfProfile,
      child: LogicalPlan,
      spark: ClassicSparkSession): Option[Expression] = {
    val arguments = profile.parameters.zip(udf.children).toMap
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

  /**
   * Whether a profile may be applied to this call site.
   *
   * The arity has to match — a profile registered under a name some other function also
   * uses would otherwise be bound to the wrong arguments — and only row-at-a-time Python
   * UDFs are analysed, because a pandas UDF's parameters are Series rather than values,
   * so the same source means something different.
   */
  private def usable(udf: PythonUDF, profile: UdfProfile): Boolean =
    profile.parameters.size == udf.children.size &&
      udf.evalType == UdfAnalysis.BatchedPythonUdf

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
