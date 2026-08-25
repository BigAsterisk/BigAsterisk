package org.apache.spark.sql.perfdebug

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.BindReferences
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, SparkStrategy, UnaryExecNode}

/** Logical marker for a profiling point. */
case class LatencyRelation(
    profileId: String,
    accumulator: LatencyAccumulator,
    child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  /**
   * Every column of the child, so the expensive records come back with the schema of
   * the DataFrame that was profiled rather than whatever the rest of the query needed.
   */
  override def references: org.apache.spark.sql.catalyst.expressions.AttributeSet =
    org.apache.spark.sql.catalyst.expressions.AttributeSet(child.output)

  override protected def withNewChildInternal(newChild: LogicalPlan): LatencyRelation =
    copy(child = newChild)
}

/**
 * Measures what each record cost to produce.
 *
 * The clock is read once per record inside Spark's generated code, and the interval
 * between consecutive records is the work the upstream pipeline did for the later one.
 * That is the quantity computation skew is about: not how long a task took, but which
 * record inside it was expensive.
 *
 * The row itself is materialised only when the record is expensive enough to be
 * retained, so an ordinary record costs a `nanoTime` call, a subtraction and a
 * comparison.
 *
 * The first record of each task is counted but never retained: the interval before it
 * spans pipeline start-up rather than any record's own work, and would otherwise always
 * look like the most expensive record in the query.
 */
case class LatencyExec(
    profileId: String,
    accumulator: LatencyAccumulator,
    child: SparkPlan) extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    child.asInstanceOf[CodegenSupport].inputRDDs()

  override def doProduce(ctx: CodegenContext): String =
    child.asInstanceOf[CodegenSupport].produce(ctx, this)

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val acc = ctx.addReferenceObj(
      "latencyAcc", accumulator, classOf[LatencyAccumulator].getName)
    val previous = ctx.addMutableState(
      "long", "latencyPrevious", v => s"$v = 0L;")

    // Force the upstream expressions for this row to be evaluated *before* the clock is
    // read. Spark's codegen emits an input variable's code at its first use, so without
    // this a costly upstream expression — a UDF, say — is not evaluated until the row is
    // materialised further down, and its cost lands on the *next* record instead of its
    // own. The suite pins this: the expensive record must be the one the UDF stalls on.
    val evaluateInputs = evaluateVariables(input)

    ctx.currentVars = input
    val rowEv = GenerateUnsafeProjection.createCode(
      ctx, BindReferences.bindReferences[Expression](child.output, child.output))

    // Each name must be taken once and reused: freshName yields a *new* name per call,
    // and two profiling points can be fused into the same generated stage.
    val now = ctx.freshName("latencyNow")
    val elapsed = ctx.freshName("latencyElapsed")

    s"""
       |$evaluateInputs
       |long $now = System.nanoTime();
       |if ($previous != 0L) {
       |  long $elapsed = $now - $previous;
       |  $acc.observe($elapsed);
       |  if ($acc.wouldKeep($elapsed)) {
       |    ${rowEv.code}
       |    $acc.keep($elapsed, ${rowEv.value});
       |  }
       |}
       |$previous = $now;
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val acc = accumulator
    val childOutput = child.output
    child.execute().mapPartitionsInternal { iter =>
      val project = UnsafeProjection.create(childOutput, childOutput)
      var previous = 0L
      iter.map { r =>
        val now = System.nanoTime()
        if (previous != 0L) {
          val elapsed = now - previous
          acc.observe(elapsed)
          if (acc.wouldKeep(elapsed)) acc.keep(elapsed, project(r))
        }
        previous = now
        r
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): LatencyExec =
    copy(child = newChild)
}

/** Plans [[LatencyRelation]] as [[LatencyExec]]. */
object LatencyStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case l: LatencyRelation =>
      LatencyExec(l.profileId, l.accumulator, planLater(l.child)) :: Nil
    case _ => Nil
  }
}

/** Installs latency profiling into a session. */
class PerfDebugExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(_ => LatencyStrategy)
  }
}
