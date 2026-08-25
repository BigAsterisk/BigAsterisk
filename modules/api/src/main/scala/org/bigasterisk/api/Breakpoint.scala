package org.bigasterisk.api

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType

/**
 * A simulated breakpoint: a point in a query whose program state can be inspected on
 * demand, without pausing anything.
 *
 * Stepping through a distributed job the way a conventional debugger steps through a
 * program is not affordable — halting every executor to look at one intermediate value
 * would throw away the throughput the job exists for. A *simulated* breakpoint gives
 * the same experience without the halt: it records what is needed to regenerate the
 * state at that point from the last materialization point, and regenerates it when, and
 * only when, someone looks.
 *
 * Setting one therefore costs nothing. Nothing is captured while the query runs, no
 * operator is inserted, and a breakpoint that is never inspected is free.
 *
 * @group breakpoint
 */
trait Breakpoint {

  /** Identifier, unique within the session. */
  def id: String

  /**
   * The query as it stands at this point.
   *
   * Build the rest of the query on this. It is the DataFrame that was passed in — a
   * breakpoint does not change the query it is set on.
   */
  def df: DataFrame

  /** The shape of the state here, available without computing it. */
  def schema: StructType

  /**
   * The program state at this point: the records that would be flowing past it.
   *
   * Regenerated on demand. When [[materialize]] has been called it is served from
   * there; otherwise the query prefix up to this point is re-executed, which is what
   * "regenerate from the last materialization point" means when nothing nearer has been
   * pinned.
   *
   * @param limit how many records to bring back
   */
  def state(limit: Int = 20): Array[Row]

  /** How many records pass this point. */
  def count(): Long

  /**
   * Pins the state here, so repeated inspection and any resumed execution start from it
   * rather than re-running the prefix.
   *
   * This is the trade the technique is built on: a breakpoint costs nothing until you
   * decide you are going to look more than once.
   */
  def materialize(): Unit

  /** True when the state here has been pinned. */
  def isMaterialized: Boolean

  /** Unpins the state. */
  def release(): Unit

  /**
   * Resumes the computation from this point, through `continue`.
   *
   * The point of resuming through a function rather than simply continuing is that
   * `continue` need not be what the original query did. Correcting the step that
   * follows a breakpoint and re-running from there — rather than from the beginning —
   * is the on-the-fly fix the technique is for.
   *
   * {{{
   * // the original step was wrong; resume from the breakpoint with it corrected
   * bp.resumeWith(_.filter(col("amount") > 0).groupBy("cid").sum("amount"))
   * }}}
   */
  def resumeWith(continue: DataFrame => DataFrame): DataFrame
}

/**
 * Simulated breakpoints over a query.
 *
 * Obtain an instance from [[BigAsterisk.breakpoints]].
 *
 * {{{
 * val bp = BigAsterisk.breakpoints(spark).breakpoint(orders.filter(col("amount") > 100))
 *
 * // the rest of the query is built on the breakpoint, and runs at full speed
 * bp.df.groupBy("cid").sum("amount").collect()
 *
 * // afterwards, look at what was flowing past that point
 * bp.state().foreach(println)
 * }}}
 *
 * @group breakpoint
 */
trait BreakpointSupport {

  /** Sets a breakpoint at `df`. Costs nothing until the state is inspected. */
  def breakpoint(df: DataFrame): Breakpoint

  /** Every breakpoint set in this session that has not been cleared. */
  def active: Seq[Breakpoint]

  /** Removes every breakpoint, releasing anything they pinned. */
  def clear(): Unit
}
