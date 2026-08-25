package org.bigasterisk.api

import org.apache.spark.sql.{Column, DataFrame, Row}

/**
 * A guard placed on intermediate data: rows flowing through a point in a query are
 * tested against a condition, and the ones that match are counted and sampled back to
 * the driver.
 *
 * This is BigDebug's on-demand watchpoint (ICSE 2016) — the distributed counterpart of
 * a watchpoint in a conventional debugger. Instead of inspecting a variable at a line
 * of code, you inspect the records passing a stage of a distributed job, without
 * collecting the whole intermediate dataset.
 *
 * @group watchpoint
 */
trait Watchpoint {

  /** Identifier, unique within the session. Appears in the Spark UI as the accumulator name. */
  def id: String

  /** The guard, as SQL text. */
  def condition: String

  /**
   * The instrumented DataFrame. Build the rest of the query on this rather than on the
   * DataFrame you passed to `watch`, or nothing will be observed.
   */
  def df: DataFrame

  /**
   * How many rows matched the condition.
   *
   * Counted in full even when more rows matched than [[capacity]] allows to be kept,
   * so this is the true selectivity of the guard rather than a sample size.
   */
  def hits: Long

  /** The matching rows retained, up to [[capacity]]. */
  def captured: Array[Row]

  /**
   * The same rows as [[captured]], each serialised as a JSON object.
   *
   * Provided for language bindings that cannot marshal a Spark `Row` across the
   * process boundary — the PySpark front end reads this.
   */
  def capturedJson: Array[String]

  /** The most rows this watchpoint will bring back to the driver. */
  def capacity: Int

  /** True when more rows matched than were kept — [[captured]] is a sample. */
  final def truncated: Boolean = hits > captured.length

  /** Discards what has been observed so far, so the watchpoint can be reused for another run. */
  def reset(): Unit
}

/**
 * On-demand watchpoints over the intermediate data of a Spark SQL query.
 *
 * Obtain an instance from [[BigAsterisk.watchpoints]].
 *
 * {{{
 * val orders = spark.table("orders")
 * val wp = BigAsterisk.watchpoints(spark).watch(orders, col("amount") > 10000)
 *
 * // build the rest of the query on the instrumented DataFrame
 * wp.df.groupBy("cid").sum("amount").collect()
 *
 * println(s"\${wp.hits} suspicious rows")
 * wp.captured.foreach(println)
 * }}}
 *
 * @group watchpoint
 */
trait WatchpointSupport {

  /**
   * Places a watchpoint on the rows flowing out of `df`.
   *
   * Evaluation happens on the executors and is fused into Spark's generated code, so a
   * watchpoint costs one predicate per row and moves no data until something matches.
   * Only the first `capacity` matching rows are brought back; the rest are counted.
   *
   * @param capacity the most matching rows to retain. Keep it small: these rows are
   *        held on the driver.
   */
  def watch(df: DataFrame, condition: Column, capacity: Int = 1000): Watchpoint

  /** Every watchpoint created in this session that has not been cleared. */
  def active: Seq[Watchpoint]

  /** Forgets every watchpoint. Already-instrumented DataFrames keep working. */
  def clear(): Unit
}
