package org.apache.spark.sql.perfdebug

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.util.AccumulatorV2

/** What a profile measured: totals, and the most expensive records kept. */
case class LatencyValue(records: Long, totalNanos: Long, slowest: Seq[(Long, UnsafeRow)])

/**
 * Carries per-record timings from the executors back to the driver, keeping only the
 * most expensive records.
 *
 * Retaining every timing would cost as much as the query. Instead the totals are exact
 * — every record is counted and its cost summed, so the mean is exact — while only the
 * `topK` most expensive records are materialised and shipped. That is the asymmetry
 * computation skew has: the interesting records are few.
 *
 * Spark merges accumulator updates only from tasks that succeed, so speculative and
 * retried attempts do not double-count. Re-running the same query adds to the previous
 * totals; call `reset` between runs.
 */
class LatencyAccumulator(val topK: Int)
  extends AccumulatorV2[(Long, UnsafeRow), LatencyValue] {

  require(topK >= 0, s"topK must not be negative, got $topK")

  private var count: Long = 0L
  private var total: Long = 0L

  // Ordered by cost ascending, so the cheapest kept record — the one to evict — is at
  // the head. A heap would be asymptotically better; with topK in the tens, an ordered
  // buffer is faster and simpler.
  private val kept = mutable.ArrayBuffer.empty[(Long, UnsafeRow)]

  override def isZero: Boolean = count == 0L && kept.isEmpty

  override def copy(): LatencyAccumulator = {
    val other = new LatencyAccumulator(topK)
    other.count = count
    other.total = total
    other.kept ++= kept
    other
  }

  override def reset(): Unit = {
    count = 0L
    total = 0L
    kept.clear()
  }

  /**
   * Counts a record's cost without retaining it.
   *
   * Called for every record, so it must stay allocation-free.
   */
  def observe(nanos: Long): Unit = {
    count += 1L
    total += nanos
  }

  /**
   * Whether a record costing `nanos` belongs in the retained set.
   *
   * Checked before the row is materialised, so a record that is not expensive enough
   * costs one comparison and no allocation.
   */
  def wouldKeep(nanos: Long): Boolean =
    topK > 0 && (kept.length < topK || nanos > kept.head._1)

  /** Retains a record, evicting the cheapest kept one if the set is full. */
  def keep(nanos: Long, row: UnsafeRow): Unit = {
    // `copy()` is mandatory: the row handed to us is the operator's reusable buffer.
    insert((nanos, row.copy()))
    while (kept.length > topK) kept.remove(0)
  }

  private def insert(entry: (Long, UnsafeRow)): Unit = {
    var i = 0
    while (i < kept.length && kept(i)._1 <= entry._1) i += 1
    kept.insert(i, entry)
  }

  override def add(v: (Long, UnsafeRow)): Unit = {
    observe(v._1)
    if (wouldKeep(v._1)) keep(v._1, v._2)
  }

  override def merge(other: AccumulatorV2[(Long, UnsafeRow), LatencyValue]): Unit =
    other match {
      case o: LatencyAccumulator =>
        count += o.count
        total += o.total
        o.kept.foreach(insert)
        while (kept.length > topK) kept.remove(0)
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${getClass.getSimpleName} with ${other.getClass.getSimpleName}")
    }

  /** Totals, and the retained records most expensive first. */
  override def value: LatencyValue = LatencyValue(count, total, kept.reverse.toSeq)
}
