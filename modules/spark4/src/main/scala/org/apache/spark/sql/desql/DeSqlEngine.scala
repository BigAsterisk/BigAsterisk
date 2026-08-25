package org.apache.spark.sql.desql

import scala.collection.mutable

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.classic.{Dataset => ClassicDataset, SparkSession => ClassicSparkSession}
import org.apache.spark.sql.types.StructType

import org.bigasterisk.api.{DeSqlSupport, QueryStep}

/**
 * Step-through debugging for Spark SQL, implemented over stock Catalyst.
 *
 * ==Approach==
 * The analyzed logical plan of a query is a tree whose every subtree is itself a
 * complete, resolved query: attributes flow strictly bottom-up, so the subtree rooted
 * at any node computes exactly "the query so far". Decomposition is therefore a
 * post-order walk that re-wraps each node as its own `DataFrame`, and the intermediate
 * data at a step is simply that DataFrame's rows.
 *
 * The original DeSQL (FSE 2024) obtained the same decomposition by adding a
 * `mappingIndex` field and visitor hooks to Catalyst's own classes, which required
 * forking Spark. Re-deriving the decomposition from the unmodified analyzed plan gives
 * the same steps without a fork, which is why this is a reimplementation rather than a
 * port. See `PROVENANCE.md`.
 *
 * ==Which nodes become steps==
 * Nodes that do not change the data are folded away, so the steps line up with parts a
 * reader would recognise in their query rather than with Catalyst bookkeeping.
 * Correlated subqueries are excluded: their plans carry outer references and are not
 * independently executable.
 */
class DeSqlEngine extends DeSqlSupport {

  override def decompose(df: DataFrame): Seq[QueryStep] = {
    val spark = df.sparkSession
    val classic = spark match {
      case c: ClassicSparkSession => c
      case other =>
        throw new UnsupportedOperationException(
          "Step-through SQL debugging needs a classic SparkSession; this one is " +
          s"${other.getClass.getName}. Spark Connect sessions are not supported: the " +
          "decomposition runs against the driver-side analyzed plan, which a Connect " +
          "client does not hold.")
    }

    DeSqlEngine.stepNodes(df.queryExecution.analyzed).zipWithIndex.map {
      case (node, id) =>
        new Spark4QueryStep(
          id = id,
          operator = node.operator,
          detail = node.detail,
          childIds = node.childIndexes,
          plan = node.plan,
          classic = classic)
    }
  }
}

object DeSqlEngine {

  /** One retained node of a decomposition: the plan, and how to describe it. */
  case class StepNode(plan: LogicalPlan, operator: String, detail: String, childIndexes: Seq[Int])

  /**
   * Walks `analyzed` and returns the nodes that become steps, in post-order: every
   * node appears after the nodes feeding it.
   *
   * Shared with Vega, so both tools agree on what counts as a part of a query — a
   * reusable materialization point is the same thing as an inspectable step.
   */
  def stepNodes(analyzed: LogicalPlan): Seq[StepNode] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[StepNode]
    val seen = scala.collection.mutable.LinkedHashMap.empty[LogicalPlan, Int]

    def visit(plan: LogicalPlan, labels: List[String]): Int = seen.getOrElseUpdate(plan, {
      if (isTransparent(plan)) {
        visit(plan.children.head, labels ++ nameOf(plan))
      } else {
        val childIndexes = plan.children.map(visit(_, Nil))
        out += StepNode(plan, operatorOf(plan), describe(plan, labels), childIndexes)
        out.length - 1
      }
    })

    visit(analyzed, Nil)
    out.toSeq
  }


  /**
   * True for nodes that pass their input through unchanged. Folding these away keeps
   * the decomposition at the level of the user's query rather than Catalyst's: a
   * `FROM orders o` contributes a `SubqueryAlias` and, for a temp view, a `View`, and
   * neither is a part of the query a reader would point at.
   */
  def isTransparent(plan: LogicalPlan): Boolean = plan match {
    case _: SubqueryAlias => true
    case _: View          => true
    case _                => false
  }

  /** The name a transparent wrapper carries, to be attached to the node beneath it. */
  def nameOf(plan: LogicalPlan): Option[String] = plan match {
    case a: SubqueryAlias => Some(a.identifier.name)
    case v: View          => Some(v.desc.identifier.table)
    case _                => None
  }

  /** The operator name to report. Catalyst's internal names are tidied where they leak. */
  def operatorOf(plan: LogicalPlan): String = plan match {
    case _: LeafNode if plan.nodeName.endsWith("Relation") => "Relation"
    case _                                                 => plan.nodeName
  }

  /** A readable rendering of what the operator does, using SQL text where possible. */
  def describe(plan: LogicalPlan, labels: List[String] = Nil): String = plan match {
    case f: Filter =>
      sql(f.condition)
    case j: Join =>
      val on = j.condition.map(c => s" ON ${sql(c)}").getOrElse("")
      s"${j.joinType.sql}$on"
    case a: Aggregate =>
      val by =
        if (a.groupingExpressions.isEmpty) ""
        else s" GROUP BY ${a.groupingExpressions.map(sql).mkString(", ")}"
      s"${a.aggregateExpressions.map(sql).mkString(", ")}$by"
    case p: Project =>
      p.projectList.map(sql).mkString(", ")
    case s: Sort =>
      s.order.map(sql).mkString(", ")
    case l: LocalLimit =>
      sql(l.limitExpr)
    case g: GlobalLimit =>
      sql(g.limitExpr)
    case w: Window =>
      w.windowExpressions.map(sql).mkString(", ")
    case d: Deduplicate =>
      d.keys.map(sql).mkString(", ")
    case _: Union =>
      "UNION ALL"
    case _: LeafNode =>
      // the table or view this scan reads, named by the wrappers folded away above it
      relationName(labels).getOrElse(plan.simpleString(maxFields = 4))
    case other =>
      other.simpleString(maxFields = 4)
  }

  /**
   * Renders a scan's identity from the wrapper names folded away above it, which
   * arrive outermost first: the innermost is the table or view, the outermost is the
   * alias the query used. Duplicates collapse, so an unaliased table reads as just its
   * own name.
   */
  def relationName(labels: List[String]): Option[String] = labels.distinct match {
    case Nil            => None
    case single :: Nil  => Some(single)
    case alias :: rest  => Some(s"${rest.last} AS $alias")
  }

  /**
   * SQL text for an expression, falling back to its `toString` when Catalyst cannot
   * render it (some internal expressions have no SQL form).
   */
  private def sql(e: Expression): String =
    try e.sql
    catch { case _: Throwable => e.toString }

  /** True if the plan reads attributes from an enclosing query. */
  def hasOuterReferences(plan: LogicalPlan): Boolean =
    plan.expressions.exists(SubqueryExpression.hasCorrelatedSubquery) ||
      plan.expressions.exists(_.exists(_.isInstanceOf[org.apache.spark.sql.catalyst.expressions.OuterReference]))
}

/** A [[QueryStep]] backed by one node of the analyzed plan. */
private[desql] class Spark4QueryStep(
    override val id: Int,
    override val operator: String,
    override val detail: String,
    override val childIds: Seq[Int],
    private val plan: LogicalPlan,
    private val classic: ClassicSparkSession) extends QueryStep {

  override def schema: StructType = plan.schema

  override lazy val data: DataFrame = {
    if (DeSqlEngine.hasOuterReferences(plan)) {
      throw new UnsupportedOperationException(
        s"Step $id ($operator) is part of a correlated subquery: its plan reads " +
        "attributes from the enclosing query, so it cannot be executed on its own. " +
        "Inspect the enclosing step instead.")
    }
    ClassicDataset.ofRows(classic, plan)
  }

  override def toString: String =
    s"[$id] $operator ${if (detail.isEmpty) "" else s"— $detail"}"
}
