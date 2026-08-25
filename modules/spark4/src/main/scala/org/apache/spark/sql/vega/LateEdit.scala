package org.apache.spark.sql.vega

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Vega's second optimization: rewrite a query so a modification happens as late as
 * possible.
 *
 * ==Why==
 * Reuse across revisions is worth only as much as the prefix the two revisions share.
 * An edit near the sources destroys almost all of it: change a `WHERE` on a scanned
 * table and everything above that filter — including an expensive join computed last
 * time — has to be recomputed, even though the join itself did not change.
 *
 * Pulling the filter *up* past the join fixes that. The join below is then identical to
 * the one the previous revision materialised, and the edited filter applies to its
 * result instead.
 *
 * {{{
 * // as written: the edit sits below the join, so the join cannot be reused
 * Join(Filter(amount > 200, orders), customers)
 *
 * // rewritten: the join is unchanged and reusable, the edit applies above it
 * Filter(amount > 200, Join(orders, customers))
 * }}}
 *
 * A plain `WHERE` needs no help. An *analysed* plan puts it above the join already, in
 * the order the query states — pushing it down is something Spark's optimizer does
 * later, and Vega matches on analysed plans. The shape this rewrite exists for is a
 * filter the developer wrote lower, inside a derived table or a CTE, which analysis
 * really does place beneath the join.
 *
 * ==When it is legal==
 * Only where the rewrite cannot change the answer. Spark's own optimizer pushes filters
 * the other way for speed; pulling one up is the same equivalence read backwards, and
 * the same conditions govern it.
 *
 *   - '''Inner join''' — a filter on either side commutes with the join, because an
 *     inner join emits a row only when both sides contributed.
 *   - '''Outer join, preserved side only''' — a filter on the side whose rows are kept
 *     commutes, since those columns survive the join untouched. On the null-supplying
 *     side it does not: filtering before the join drops a row, filtering after keeps it
 *     with nulls.
 *   - '''Project''' — only when the projection carries the filter's columns through
 *     unchanged, so the predicate still means the same thing above it.
 *
 * Anything else is left alone. In particular a filter is never pulled through an
 * aggregation: filtering rows before grouping and filtering groups afterwards are
 * different queries.
 */
object LateEdit {

  /** How many rewrite passes to run before giving up on reaching a fixpoint. */
  private val MaxPasses = 8

  /**
   * Pulls filters up as far as is legal.
   *
   * Returns the plan unchanged when nothing can move, so a caller can compare by
   * reference to tell whether a rewrite happened.
   */
  def pullUpFilters(plan: LogicalPlan): LogicalPlan = {
    // Moving an edit later only pays when there is expensive work to get past, and in a
    // relational plan that means a join. Without one, rewriting would shuffle a filter
    // past a projection for no gain while changing which subtree Vega materialises —
    // churn, and a larger intermediate for nothing.
    if (!plan.exists(_.isInstanceOf[Join])) return plan

    var current = plan
    var pass = 0
    var changed = true
    while (changed && pass < MaxPasses) {
      val next = onePass(current)
      changed = !next.fastEquals(current)
      current = next
      pass += 1
    }
    current
  }

  private def onePass(plan: LogicalPlan): LogicalPlan = plan transformUp {

    // a filter on the left input, where the left rows survive the join
    case j @ Join(Filter(condition, left), right, joinType, _, _)
        if preservesLeft(joinType) && references(condition).subsetOf(AttributeSet(left.output)) =>
      Filter(condition, j.copy(left = left))

    // a filter on the right input, where the right rows survive the join
    case j @ Join(left, Filter(condition, right), joinType, _, _)
        if preservesRight(joinType) && references(condition).subsetOf(AttributeSet(right.output)) =>
      Filter(condition, j.copy(right = right))

    // a projection that carries the filter's columns through untouched
    case p @ Project(projectList, Filter(condition, child))
        if carriesThrough(projectList, condition) =>
      Filter(condition, p.copy(child = child))

    // naming a relation cannot change its rows, so a filter always commutes with an
    // alias. This is what lets a filter written inside a derived table reach the join
    // above it: analysis wraps one as SubqueryAlias(Project(Filter(...))).
    case a @ SubqueryAlias(_, Filter(condition, child)) =>
      Filter(condition, a.copy(child = child))
  }

  /** An inner join keeps only matched rows, so a filter on either side commutes. */
  private def preservesLeft(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftOuter => true
    case _                 => false
  }

  private def preservesRight(joinType: JoinType): Boolean = joinType match {
    case Inner | RightOuter => true
    case _                  => false
  }

  /**
   * True when every column the condition reads survives the projection under the same
   * name and meaning — that is, is projected as a bare attribute rather than computed.
   */
  private def carriesThrough(projectList: Seq[NamedExpression], condition: Expression): Boolean = {
    val passedThrough = AttributeSet(projectList.collect { case a: Attribute => a })
    references(condition).nonEmpty && references(condition).subsetOf(passedThrough)
  }

  private def references(condition: Expression): AttributeSet = condition.references
}
