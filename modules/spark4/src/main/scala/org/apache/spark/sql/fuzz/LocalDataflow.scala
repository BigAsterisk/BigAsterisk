package org.apache.spark.sql.fuzz

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.StructType

/**
 * The dataflow's semantics, evaluated without Spark.
 *
 * ==Why==
 * A fuzzing campaign runs the same small query thousands of times. Run through Spark,
 * almost all of that is framework: planning, scheduling, task serialization, shuffle
 * setup — work that dwarfs the query itself when the input is twenty rows. Removing it
 * is the whole point of framework abstraction, and it is what makes a campaign of a
 * hundred thousand iterations affordable rather than theoretical.
 *
 * This interprets the analyzed plan directly over in-memory rows, using Catalyst's own
 * expression evaluation. The operator semantics are Spark's; only the framework around
 * them is gone.
 *
 * ==Fidelity==
 * A faster oracle that disagrees with the real one is worthless, so this refuses rather
 * than approximates. Any operator or aggregate outside the supported set returns
 * [[Unsupported]] and the caller falls back to Spark for that query. The suite pins
 * agreement differentially: for every supported shape, interpreting and executing must
 * produce the same rows.
 */
object LocalDataflow {

  /** Raised internally when the plan leaves the supported set. */
  private class UnsupportedPlan(val reason: String) extends RuntimeException(reason)

  /** What happened when a plan was interpreted. */
  sealed trait Outcome
  /** The query ran and produced these rows. */
  final case class Rows(rows: Seq[Row]) extends Outcome
  /** The query threw, exactly as it would have under Spark. */
  final case class Failed(error: Throwable) extends Outcome
  /** The plan is outside the interpreter's set; run it on Spark instead. */
  final case class Unsupported(reason: String) extends Outcome

  /**
   * Interprets `plan`, reading each leaf relation from `tables`.
   *
   * @param tables rows for each leaf, keyed by the identity [[leafKey]] assigns
   */
  def evaluate(plan: LogicalPlan, tables: Map[String, Seq[Row]]): Outcome =
    try {
      val schema = plan.schema
      val internal = eval(plan, tables)
      val toExternal = CatalystTypeConverters.createToScalaConverter(schema)
      Rows(internal.map(r => toExternal(r).asInstanceOf[Row]))
    } catch {
      case u: UnsupportedPlan => Unsupported(u.reason)
      case NonFatal(e)        => Failed(e)
    }

  /**
   * The identity a leaf relation is looked up by.
   *
   * Leaves are matched on their output attribute ids, which are stable for one analyzed
   * plan — the same plan is interpreted with different data on every iteration, so the
   * mapping only has to hold within that plan.
   */
  def leafKey(leaf: LogicalPlan): String =
    leaf.output.map(_.exprId.id).mkString(",")

  /** Every leaf of `plan` that data must be supplied for. */
  def leaves(plan: LogicalPlan): Seq[LogicalPlan] =
    plan.collect { case l: LeafNode => l }

  private def unsupported(what: String): Nothing = throw new UnsupportedPlan(what)

  private def eval(plan: LogicalPlan, tables: Map[String, Seq[Row]]): Seq[InternalRow] =
    plan match {
      case leaf: LeafNode =>
        val key = leafKey(leaf)
        val rows = tables.getOrElse(key, unsupported(s"no data supplied for ${leaf.nodeName}"))
        val toCatalyst = CatalystTypeConverters.createToCatalystConverter(
          StructType.fromAttributes(leaf.output))
        rows.map(r => toCatalyst(r).asInstanceOf[InternalRow])

      // wrappers that do not change the rows
      case a: SubqueryAlias => eval(a.child, tables)
      case v: View          => eval(v.child, tables)

      case Project(projectList, child) =>
        val input = eval(child, tables)
        val project = InterpretedMutableProjection.createProjection(
          bind(projectList, child.output))
        input.map(project(_).copy())

      case Filter(condition, child) =>
        val input = eval(child, tables)
        val predicate = Predicate.createInterpreted(bind(condition, child.output))
        input.filter(predicate.eval)

      case Union(children, _, _) =>
        children.flatMap(eval(_, tables))

      case GlobalLimit(limitExpr, child) =>
        eval(child, tables).take(intOf(limitExpr))
      case LocalLimit(limitExpr, child) =>
        eval(child, tables).take(intOf(limitExpr))

      case Sort(order, _, child, _) =>
        val input = eval(child, tables)
        val ordering = InterpretedOrdering.forSchema(child.output.map(_.dataType))
        val keyProjection = InterpretedMutableProjection.createProjection(
          bind(order.map(_.child), child.output))
        // stable sort on the projected keys; direction handled per key below
        val keyed = input.map(r => (keyProjection(r).copy(), r))
        val comparator = new InterpretedOrdering(
          order.zipWithIndex.map { case (o, i) =>
            SortOrder(BoundReference(i, o.child.dataType, o.child.nullable), o.direction,
              o.nullOrdering, Seq.empty)
          })
        keyed.sortWith((a, b) => comparator.compare(a._1, b._1) < 0).map(_._2)
        // `ordering` is unused; kept out of the comparison path deliberately
          .ensuring(_ => ordering != null || true)

      case Deduplicate(keys, child) =>
        val input = eval(child, tables)
        val keyProjection = InterpretedMutableProjection.createProjection(
          bind(keys.map(_.asInstanceOf[Expression]), child.output))
        val seen = mutable.LinkedHashSet.empty[InternalRow]
        val out = mutable.ArrayBuffer.empty[InternalRow]
        input.foreach { r =>
          val key = keyProjection(r).copy()
          if (seen.add(key)) out += r
        }
        out.toSeq

      case Join(left, right, joinType, condition, _) =>
        joinInner(joinType, left, right, condition, tables)

      case aggregate: Aggregate =>
        evalAggregate(aggregate, tables)

      case other =>
        unsupported(other.nodeName)
    }

  /** Inner joins only: everything else changes row multiplicity in ways worth refusing. */
  private def joinInner(
      joinType: JoinType,
      left: LogicalPlan,
      right: LogicalPlan,
      condition: Option[Expression],
      tables: Map[String, Seq[Row]]): Seq[InternalRow] = {
    if (joinType != Inner) unsupported(s"${joinType.sql} join")
    val leftRows = eval(left, tables)
    val rightRows = eval(right, tables)
    val combined = left.output ++ right.output
    val predicate = condition
      .map(c => Predicate.createInterpreted(bind(c, combined)))
      .getOrElse(Predicate.createInterpreted(Literal(true)))

    val out = mutable.ArrayBuffer.empty[InternalRow]
    leftRows.foreach { l =>
      rightRows.foreach { r =>
        val joined = new JoinedRow(l, r)
        if (predicate.eval(joined)) out += joined.copy()
      }
    }
    out.toSeq
  }

  /**
   * Grouped aggregation, using Catalyst's own declarative aggregate definitions.
   *
   * `Sum`, `Count`, `Max`, `Min` and `Average` are all `DeclarativeAggregate`s: each
   * supplies the expressions for initialising, updating and finalising its buffer, so
   * running them is a matter of evaluating those rather than reimplementing the
   * arithmetic. Anything imperative is refused.
   */
  private def evalAggregate(
      aggregate: Aggregate,
      tables: Map[String, Seq[Row]]): Seq[InternalRow] = {
    val child = aggregate.child
    val input = eval(child, tables)

    val aggregateExpressions = aggregate.aggregateExpressions
      .flatMap(_.collect { case a: AggregateExpression => a })
      .distinct
    val functions = aggregateExpressions.map { a =>
      a.aggregateFunction match {
        case d: DeclarativeAggregate => d
        case other => unsupported(s"aggregate ${other.nodeName}")
      }
    }
    if (aggregateExpressions.exists(_.isDistinct)) unsupported("DISTINCT aggregate")

    val groupingProjection = InterpretedMutableProjection.createProjection(
      bind(aggregate.groupingExpressions, child.output))

    // group rows, preserving first-seen order so results are deterministic
    val groups = mutable.LinkedHashMap.empty[InternalRow, mutable.ArrayBuffer[InternalRow]]
    if (aggregate.groupingExpressions.isEmpty) {
      groups(InternalRow.empty) = mutable.ArrayBuffer.empty ++= input
    } else {
      input.foreach { r =>
        val key = groupingProjection(r).copy()
        groups.getOrElseUpdate(key, mutable.ArrayBuffer.empty) += r
      }
    }

    val bufferAttributes = functions.flatMap(_.aggBufferAttributes)
    val initial = InterpretedMutableProjection.createProjection(
      functions.flatMap(_.initialValues))
    val update = InterpretedMutableProjection.createProjection(
      bind(functions.flatMap(_.updateExpressions), bufferAttributes ++ child.output))
    val finalise = InterpretedMutableProjection.createProjection(
      bind(functions.map(_.evaluateExpression), bufferAttributes))

    // the result row is the aggregate expressions evaluated over (grouping ++ results)
    val resultInput = aggregate.groupingExpressions.map(toAttribute) ++
      aggregateExpressions.zip(functions).map { case (a, f) =>
        AttributeReference(s"agg${a.hashCode()}", f.dataType, f.nullable)()
      }
    val rewritten = aggregate.aggregateExpressions.map { e =>
      e.transform {
        case a: AggregateExpression =>
          val i = aggregateExpressions.indexOf(a)
          resultInput(aggregate.groupingExpressions.size + i)
        case e if aggregate.groupingExpressions.exists(_.semanticEquals(e)) =>
          resultInput(aggregate.groupingExpressions.indexWhere(_.semanticEquals(e)))
      }.asInstanceOf[Expression]
    }
    val result = InterpretedMutableProjection.createProjection(bind(rewritten, resultInput))

    groups.toSeq.map { case (key, rows) =>
      var buffer: InternalRow = initial(InternalRow.empty).copy()
      rows.foreach { r => buffer = update(new JoinedRow(buffer, r)).copy() }
      val finalised = finalise(buffer).copy()
      result(new JoinedRow(key, finalised)).copy()
    }
  }

  private def toAttribute(e: Expression): Attribute = e match {
    case a: Attribute => a
    case other        => AttributeReference(s"grp${other.hashCode()}", other.dataType, other.nullable)()
  }

  private def bind[A <: Expression](exprs: Seq[A], input: Seq[Attribute]): Seq[Expression] =
    exprs.map(e => BindReferences.bindReference(e.asInstanceOf[Expression], input))

  private def bind(expr: Expression, input: Seq[Attribute]): Expression =
    BindReferences.bindReference(expr, input)

  private def intOf(e: Expression): Int = e match {
    case Literal(v: Int, _) => v
    case other              => unsupported(s"non-literal limit ${other.sql}")
  }
}
