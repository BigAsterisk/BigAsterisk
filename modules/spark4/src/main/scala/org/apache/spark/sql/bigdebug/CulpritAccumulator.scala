package org.apache.spark.sql.bigdebug

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.util.AccumulatorV2

/** What a guard saw when its task died. */
case class CulpritReport(row: UnsafeRow, partitionId: Int, recordIndex: Long, error: String)

/**
 * Carries the record in flight at the moment of failure back to the driver.
 *
 * Ordinary accumulators are merged only from tasks that *succeed*, which is precisely
 * the wrong behaviour here: the interesting task is the one that died. Spark supports
 * merging updates from failed tasks — it is how the built-in task metrics survive a
 * failure — through the `countFailedValues` flag on registration, and that is what makes
 * crash-culprit determination possible without the forked executor backend the original
 * BigDebug used. See `PROVENANCE.md`.
 *
 * The first report wins. A retried task fails on the same record, and a wide job can
 * have several tasks die on several bad records; reporting one of them with its
 * partition and position is more useful than reporting a set.
 */
class CulpritAccumulator extends AccumulatorV2[CulpritReport, Option[CulpritReport]] {

  private var report: Option[CulpritReport] = None

  override def isZero: Boolean = report.isEmpty

  override def copy(): CulpritAccumulator = {
    val other = new CulpritAccumulator
    other.report = report
    other
  }

  override def reset(): Unit = report = None

  override def add(v: CulpritReport): Unit = if (report.isEmpty) {
    // `copy()` is mandatory: the row is the operator's reusable buffer.
    report = Some(v.copy(row = v.row.copy()))
  }

  override def merge(
      other: AccumulatorV2[CulpritReport, Option[CulpritReport]]): Unit = other match {
    case o: CulpritAccumulator => if (report.isEmpty) report = o.report
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${getClass.getSimpleName} with ${other.getClass.getSimpleName}")
  }

  override def value: Option[CulpritReport] = report
}

/**
 * Per-task bookkeeping for a guard: remembers the current record, and reports it if the
 * task dies.
 *
 * Held in generated code as mutable state, so `observe` is a field assignment and a
 * counter increment — the row itself is only copied if a failure actually happens.
 */
class CulpritRecorder(accumulator: CulpritAccumulator) {

  private var current: UnsafeRow = _
  private var seen: Long = 0L
  private var armed: Boolean = false

  /**
   * Registers the failure listener for this task.
   *
   * Called from the generated class's initialisation, which runs once per partition.
   */
  def arm(): Unit = {
    if (!armed) {
      armed = true
      Option(TaskContext.get()).foreach { context =>
        context.addTaskFailureListener { (ctx, error) =>
          if (current != null) {
            accumulator.add(CulpritReport(
              current, ctx.partitionId(), seen - 1,
              s"${error.getClass.getSimpleName}: ${error.getMessage}"))
          }
        }
      }
    }
  }

  /** Records that this row is now in flight. */
  def observe(row: UnsafeRow): Unit = {
    current = row
    seen += 1L
  }
}
