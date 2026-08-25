package org.apache.spark.sql.watchpoint

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BindReferences, Expression, Predicate, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, SparkStrategy, UnaryExecNode}
import org.apache.spark.sql.SparkSessionExtensions

/**
 * Logical marker for a watchpoint. Carries the guard so the analyzer resolves it
 * against the child's output, exactly as it would a `WHERE` clause.
 */
case class WatchpointRelation(
    condition: Expression,
    watchId: String,
    accumulator: WatchpointAccumulator,
    child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  /**
   * Every column of the child, not just the ones the guard mentions.
   *
   * Without this, column pruning narrows the child to whatever the rest of the query
   * happens to need, and the captured rows come back with a different shape from the
   * DataFrame that was watched — a watchpoint on a three-column table feeding an
   * aggregation over two of them would report two-column rows. A watchpoint is a
   * debugging tool: showing the rows of the thing that was watched matters more than
   * pruning a column out of it.
   */
  override def references: AttributeSet = AttributeSet(child.output) ++ condition.references

  override protected def withNewChildInternal(newChild: LogicalPlan): WatchpointRelation =
    copy(child = newChild)
}

/**
 * The physical watchpoint: a strict row pass-through that evaluates the guard on every
 * record and reports the matches through an accumulator.
 *
 * It participates in whole-stage codegen, so the guard becomes a branch inside Spark's
 * generated loop rather than a separate pass over the data, and a row is materialised
 * only when the guard actually matches.
 */
case class WatchpointExec(
    condition: Expression,
    watchId: String,
    accumulator: WatchpointAccumulator,
    child: SparkPlan) extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    child.asInstanceOf[CodegenSupport].inputRDDs()

  override def doProduce(ctx: CodegenContext): String =
    child.asInstanceOf[CodegenSupport].produce(ctx, this)

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val accTerm = ctx.addReferenceObj(
      "watchpointAcc", accumulator, classOf[WatchpointAccumulator].getName)

    ctx.currentVars = input
    val guard = BindReferences.bindReference(condition, child.output).genCode(ctx)

    // Build the row only inside the matching branch: an unmatched record costs one
    // predicate and nothing else.
    ctx.currentVars = input
    val rowEv = GenerateUnsafeProjection.createCode(
      ctx, BindReferences.bindReferences[Expression](child.output, child.output))

    s"""
       |${guard.code}
       |if (!${guard.isNull} && ${guard.value}) {
       |  ${rowEv.code}
       |  $accTerm.add(${rowEv.value});
       |}
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    // Bind to locals: capturing `this` would drag the whole plan into the closure.
    val boundCondition = condition
    val childOutput = child.output
    val acc = accumulator
    child.execute().mapPartitionsInternal { iter =>
      val predicate = Predicate.create(boundCondition, childOutput)
      predicate.initialize(0)
      val project = UnsafeProjection.create(childOutput, childOutput)
      iter.map { r =>
        if (predicate.eval(r)) acc.add(project(r))
        r
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WatchpointExec =
    copy(child = newChild)
}

/** Plans [[WatchpointRelation]] as [[WatchpointExec]]. */
object WatchpointStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case w: WatchpointRelation =>
      WatchpointExec(w.condition, w.watchId, w.accumulator, planLater(w.child)) :: Nil
    case _ => Nil
  }
}

/**
 * Installs watchpoint planning into a session.
 *
 * Registered alongside the lineage extension through
 * [[org.bigasterisk.spark4.Spark4Binding.requiredConf]]; Spark accepts a
 * comma-separated list of extensions, so the two are independent.
 */
class WatchpointExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(_ => WatchpointStrategy)
  }
}
