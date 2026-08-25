package org.bigasterisk.api

import org.apache.spark.sql.DataFrame

/**
 * The outcome of preparing one query revision for execution.
 *
 * @group vega
 */
trait VegaRun {

  /**
   * The DataFrame to execute.
   *
   * Semantically identical to the one passed in — Vega changes how much work the query
   * does, never what it returns.
   */
  def df: DataFrame

  /**
   * Descriptions of the parts of this query served from a previous revision's
   * materialized results, deepest first.
   */
  def reused: Seq[String]

  /** Descriptions of the parts materialized during this run, for the next revision. */
  def materialized: Seq[String]

  /** How many parts the query decomposes into. */
  def steps: Int

  /** Fraction of this query's parts that came from a previous revision. */
  final def reuseRatio: Double = if (steps == 0) 0.0 else reused.size.toDouble / steps
}

/**
 * Incremental re-execution across successive revisions of a query.
 *
 * Exploratory analysis is a sequence of near-identical queries: you run one, look at
 * the answer, change a projection or add a grouping, and run it again. Each run
 * normally starts from nothing, even though most of the work is the same as last time.
 * Vega materializes the reusable parts of a query as it runs, so the next revision
 * starts from the deepest point the two still share.
 *
 * Obtain an instance from [[BigAsterisk.vega]].
 *
 * {{{
 * val vega = BigAsterisk.vega(spark)
 *
 * val v1 = vega.run(spark.sql("SELECT cid, amount FROM orders WHERE amount > 100"))
 * v1.df.collect()
 *
 * // a revision: the filter is unchanged, so its result is reused
 * val v2 = vega.run(spark.sql(
 *   "SELECT cid, SUM(amount) FROM orders WHERE amount > 100 GROUP BY cid"))
 * v2.reused        // the scan and the filter
 * v2.df.collect()
 * }}}
 *
 * From *Optimizing Interactive Development of Data-Intensive Applications* (SoCC 2016).
 *
 * @group vega
 */
trait VegaSupport {

  /**
   * Prepares `df` for execution, reusing what a previous revision already computed and
   * materializing what this one can contribute.
   *
   * Materialization costs time and memory on the run that performs it, and pays for
   * itself on the next revision — the trade-off the technique is built on. Nothing is
   * materialized above [[maxMaterialized]].
   */
  def run(df: DataFrame): VegaRun

  /** Descriptions of everything currently materialized, across all revisions. */
  def materialized: Seq[String]

  /**
   * The most query parts to hold materialized at once. Beyond this, further parts of a
   * query are executed normally rather than cached.
   */
  def maxMaterialized: Int

  /** Releases every materialized result. */
  def clear(): Unit
}
