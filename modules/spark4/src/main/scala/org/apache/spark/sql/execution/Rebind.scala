package org.apache.spark.sql.execution

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.classic.{Dataset => ClassicDataset, SparkSession => ClassicSparkSession}

import org.bigasterisk.api.{Query, RerunSupport}

/**
 * Running a query again after its inputs have changed.
 *
 * The search-based tools — fuzzing, test generation, delta debugging — work by putting
 * different data under a query's tables and running it again. With SQL text that is
 * free: `spark.sql` re-parses, and the name resolves to whatever is registered now.
 * With a DataFrame it is not, because a DataFrame is already analysed. `flights` in its
 * plan is no longer a name; it is a particular relation, with particular attribute ids
 * that everything above it refers to.
 *
 * So substitution is done on the plan. A seed's own plan is looked for inside the
 * query's plan and replaced by the new data, wrapped in a projection that re-labels the
 * new columns with the *old* attribute ids. That last part is the whole trick: the
 * join above still says `join on flights.carrier#17`, and `#17` now points at the
 * generated column of the same name. Nothing above the leaf has to change, so no
 * re-analysis is needed and no rule can quietly rewrite the query being studied.
 *
 * Two things are matched, in order:
 *
 *  - the seed DataFrame's own plan, so a pipeline built as
 *    `flights.join(carriers, ...)` can be substituted with no views registered at all;
 *  - a `SubqueryAlias` carrying the seed's name, so a pipeline built over temp views
 *    (or a SQL string) works the same way.
 */
private[spark] object Rebind {

  /**
   * The query as a DataFrame reading `substitutions` in place of its own tables.
   *
   * @param seeds the tables the query reads, by name — the original relations, used to
   *              locate the leaves to replace
   * @param substitutions what to read instead, by the same names. Names absent from
   *                      this map keep their original data.
   */
  def frame(
      spark: ClassicSparkSession,
      query: Query,
      seeds: Map[String, DataFrame],
      substitutions: Map[String, DataFrame]): DataFrame = query match {

    case Query.Sql(text) =>
      // text re-resolves by itself: the views have already been swapped by the caller
      spark.sql(text)

    case Query.Frame(df) =>
      if (substitutions.isEmpty) df
      else {
        val replaced = substitute(df.queryExecution.analyzed, seeds, substitutions)
        ClassicDataset.ofRows(spark, replaced)
      }
  }

  /** The query as a DataFrame over its original inputs. */
  def frame(spark: ClassicSparkSession, query: Query): DataFrame = query match {
    case Query.Sql(text) => spark.sql(text)
    case Query.Frame(df) => df
  }

  /**
   * The names among `seeds` this query actually reads.
   *
   * A tool that substitutes data has to know whether the substitution would land. For
   * SQL text every name lands by construction. For a DataFrame, a name that is neither
   * one of the seed plans nor a view in the plan cannot be substituted at all, and a
   * tool that went ahead anyway would report findings about data it never changed.
   */
  def substitutable(query: Query, seeds: Map[String, DataFrame]): Set[String] = query match {
    case Query.Sql(_) => seeds.keySet
    case Query.Frame(df) =>
      val plan = df.queryExecution.analyzed
      seeds.keys.filter(name => locate(plan, name, seeds(name)).nonEmpty).toSet
  }

  /**
   * Fails with an explanation if any seed cannot be substituted.
   *
   * The explanation matters more than the check. "Nothing happened" is the worst
   * possible outcome for a search tool, and the fix — pass the DataFrame the pipeline
   * was actually built from — is not guessable from a wrong answer.
   */
  def requireSubstitutable(query: Query, seeds: Map[String, DataFrame], tool: String): Unit = {
    val missing = seeds.keySet -- substitutable(query, seeds)
    if (missing.nonEmpty) {
      throw new IllegalArgumentException(
        s"$tool cannot substitute data for ${missing.toSeq.sorted.mkString(", ")}: " +
        "the query's plan does not read them. When the query is a DataFrame, each seed " +
        "must be the DataFrame the pipeline was built from (or a table it reads under " +
        "that name), because substitution replaces that plan inside this one. " +
        s"Seeds given: ${seeds.keys.toSeq.sorted.mkString(", ")}.")
    }
  }

  /** Every subtree of `plan` that is the table `name`. */
  private def locate(plan: LogicalPlan, name: String, seed: DataFrame): Seq[LogicalPlan] = {
    val seedPlan = seed.queryExecution.analyzed
    plan.collect {
      case candidate if isTable(candidate, name, seedPlan) => candidate
    }
  }

  /** True if `candidate` is the table `name`, by that name or by being the seed itself. */
  private def isTable(candidate: LogicalPlan, name: String, seedPlan: LogicalPlan): Boolean =
    candidate match {
      case SubqueryAlias(ident, _) if ident.name.equalsIgnoreCase(name) => true
      case UnresolvedRelation(parts, _, _) if parts.lastOption.exists(_.equalsIgnoreCase(name)) =>
        true
      // Otherwise the seed's own plan, by identity. Deliberately not `sameResult`:
      // two tables that merely happen to hold the same rows are not the same table, and
      // a seed passed under the wrong name should be refused rather than bound to
      // whatever it resembles.
      case other => other.fastEquals(seedPlan)
    }

  private def substitute(
      plan: LogicalPlan,
      seeds: Map[String, DataFrame],
      substitutions: Map[String, DataFrame]): LogicalPlan = {

    // Sorted, so a node matching two seeds resolves the same way on every run.
    val wanted = substitutions.keys.toSeq.sorted
    val seedPlans = seeds.map { case (n, df) => n -> df.queryExecution.analyzed }

    def replacementFor(node: LogicalPlan): Option[LogicalPlan] =
      wanted.collectFirst {
        case name if seedPlans.get(name).exists(isTable(node, name, _)) =>
          relabel(substitutions(name).queryExecution.analyzed, node.output)
      }

    // Outermost match wins, and the replacement is *not* descended into. Both matter.
    // A seed may be a derived DataFrame — `spark.table("flights").limit(2000)` — whose
    // own plan still contains the table, so matching the outer node first substitutes
    // what the caller named rather than something inside it. And a substitute is
    // commonly derived from the same table: delta debugging hands back a filtered copy
    // of it. A traversal that re-examined the replacement would find the table inside
    // its own substitute, and would not terminate.
    def rewrite(node: LogicalPlan): LogicalPlan =
      replacementFor(node).getOrElse(node.mapChildren(rewrite))

    rewrite(plan)
  }

  /**
   * `replacement`, presented under `output`'s names, types and attribute ids.
   *
   * Columns are matched by name, not by position: generated data is built from the
   * original schema, so the names are the reliable correspondence, and a positional
   * match would silently transpose two columns of the same type.
   */
  private def relabel(replacement: LogicalPlan, output: Seq[Attribute]): LogicalPlan = {
    val byName = replacement.output.map(a => a.name.toLowerCase -> a).toMap

    val projected: Seq[NamedExpression] = output.map { original =>
      val fresh = byName.getOrElse(original.name.toLowerCase,
        throw new IllegalArgumentException(
          s"substitute data has no column '${original.name}'; it has " +
          replacement.output.map(_.name).mkString(", ")))
      val typed =
        if (fresh.dataType == original.dataType) fresh
        else Cast(fresh, original.dataType)
      Alias(typed, original.name)(exprId = original.exprId, qualifier = original.qualifier)
    }

    Project(projected, replacement)
  }

}

/**
 * [[Rebind]] behind the version-neutral interface, so tools that never see Catalyst can
 * still re-run a query with data of their choosing.
 */
class Spark4Rerun extends RerunSupport {

  override def substituted(
      spark: SparkSession,
      query: Query,
      seeds: Map[String, DataFrame],
      substitutions: Map[String, DataFrame]): DataFrame =
    Rebind.frame(classic(spark), query, seeds, substitutions)

  override def frame(spark: SparkSession, query: Query): DataFrame =
    Rebind.frame(classic(spark), query)

  override def substitutable(query: Query, seeds: Map[String, DataFrame]): Set[String] =
    Rebind.substitutable(query, seeds)

  private def classic(spark: SparkSession): ClassicSparkSession = spark match {
    case c: ClassicSparkSession => c
    case other =>
      throw new UnsupportedOperationException(
        "substituting data needs a classic SparkSession; this one is " +
        s"${other.getClass.getName}. Spark Connect sessions are not supported: " +
        "substitution rewrites the driver-side analysed plan, which a Connect client " +
        "does not hold.")
  }
}
