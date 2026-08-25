package org.apache.spark.sql.watchpoint

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.util.AccumulatorV2

/** What a watchpoint observed: how many rows matched, and the ones that were kept. */
case class WatchpointValue(hits: Long, rows: Seq[UnsafeRow])

/**
 * Carries watchpoint observations from the executors back to the driver.
 *
 * Accumulators are Spark's sanctioned executor-to-driver channel, which is what lets
 * watchpoints work without the custom RPC messages the original BigDebug added to
 * Spark's `CoarseGrainedClusterMessages` (see `PROVENANCE.md`).
 *
 * Every matching row is counted, but at most `capacity` rows are retained, so a guard
 * that turns out to match a billion rows reports its true selectivity without moving a
 * billion rows to the driver.
 *
 * Spark merges accumulator updates only from tasks that succeed, so speculative and
 * retried attempts do not double-count. Re-running the same query does add to the
 * previous totals — call `reset` between runs.
 */
class WatchpointAccumulator(val capacity: Int)
  extends AccumulatorV2[UnsafeRow, WatchpointValue] {

  require(capacity >= 0, s"watchpoint capacity must not be negative, got $capacity")

  private var count: Long = 0L
  private val kept = ArrayBuffer.empty[UnsafeRow]

  override def isZero: Boolean = count == 0L && kept.isEmpty

  override def copy(): WatchpointAccumulator = {
    val other = new WatchpointAccumulator(capacity)
    other.count = count
    other.kept ++= kept
    other
  }

  override def reset(): Unit = {
    count = 0L
    kept.clear()
  }

  override def add(row: UnsafeRow): Unit = {
    count += 1L
    // `copy()` is mandatory: the row handed to us is the reusable buffer the operator
    // writes every record into, so retaining it without copying keeps one row repeated.
    if (kept.length < capacity) kept += row.copy()
  }

  override def merge(other: AccumulatorV2[UnsafeRow, WatchpointValue]): Unit = other match {
    case o: WatchpointAccumulator =>
      count += o.count
      val room = capacity - kept.length
      if (room > 0) kept ++= o.kept.take(room)
    case _ =>
      throw new UnsupportedOperationException(
        s"Cannot merge ${getClass.getSimpleName} with ${other.getClass.getSimpleName}")
  }

  override def value: WatchpointValue = WatchpointValue(count, kept.toSeq)
}
