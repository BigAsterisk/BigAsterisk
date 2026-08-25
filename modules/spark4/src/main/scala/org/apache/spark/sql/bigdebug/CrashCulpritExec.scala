package org.apache.spark.sql.bigdebug

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, BindReferences, Expression, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, SparkStrategy, UnaryExecNode}

/** Logical marker for a crash-culprit guard. */
case class CrashCulpritRelation(
    guardId: String,
    accumulator: CulpritAccumulator,
    child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  /**
   * Every column of the child, so the culprit comes back with the schema of the
   * DataFrame that was guarded rather than whatever the rest of the query needed.
   */
  override def references: AttributeSet = AttributeSet(child.output)

  override protected def withNewChildInternal(newChild: LogicalPlan): CrashCulpritRelation =
    copy(child = newChild)
}

/**
 * Remembers the record in flight, so a failure downstream can name it.
 *
 * Per record this writes the row into a projection's reused buffer and assigns a field
 * — no allocation, and nothing moves to the driver unless the task actually dies. The
 * row is copied only in the failure listener.
 *
 * The record reported is the last one this operator emitted. That is the culprit
 * whether the exception is thrown here or anywhere downstream in the same pipeline,
 * since a stage processes one record at a time.
 */
case class CrashCulpritExec(
    guardId: String,
    accumulator: CulpritAccumulator,
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
      "culpritAcc", accumulator, classOf[CulpritAccumulator].getName)
    // One recorder per generated class, i.e. per task. Arming it here registers the
    // failure listener exactly once, at partition start.
    val recorder = ctx.addMutableState(
      classOf[CulpritRecorder].getName, "culpritRecorder",
      v => s"$v = new ${classOf[CulpritRecorder].getName}($acc); $v.arm();")

    ctx.currentVars = input
    val rowEv = GenerateUnsafeProjection.createCode(
      ctx, BindReferences.bindReferences[Expression](child.output, child.output))

    s"""
       |${rowEv.code}
       |$recorder.observe(${rowEv.value});
       |${consume(ctx, input)}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val acc = accumulator
    val childOutput = child.output
    child.execute().mapPartitionsInternal { iter =>
      val recorder = new CulpritRecorder(acc)
      recorder.arm()
      val project = UnsafeProjection.create(childOutput, childOutput)
      iter.map { r =>
        recorder.observe(project(r))
        r
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CrashCulpritExec =
    copy(child = newChild)
}

/** Plans [[CrashCulpritRelation]] as [[CrashCulpritExec]]. */
object CrashCulpritStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case c: CrashCulpritRelation =>
      CrashCulpritExec(c.guardId, c.accumulator, planLater(c.child)) :: Nil
    case _ => Nil
  }
}
