package org.bigasterisk.api

import org.apache.spark.sql.{DataFrame, Row}

/**
 * One input record, and how much it influenced a result.
 *
 * @param row    the record, as it entered the aggregation
 * @param score  influence in `[0, 1]`; the scores of a result's records sum to 1
 * @param reason why the record scored what it did, in terms of the aggregate's
 *               semantics — `"only the maximum influences"`, `"contribution 0.94 of the
 *               total"`, and so on
 *
 * @group influence
 */
case class Influence(row: Row, score: Double, reason: String) {
  override def toString: String = f"$score%.4f  $row  ($reason)"
}

/**
 * Influence-based provenance: of the records behind a result, which ones actually
 * mattered.
 *
 * Ordinary provenance answers a yes/no question — did this record contribute? For a
 * many-to-one dependency that answer is nearly useless: every record of a group
 * contributed to its aggregate, so tracing a wrong `MAX` over a million-row group
 * returns a million records.
 *
 * Influence-based provenance asks *how much* each contributed, by reading the
 * aggregate's semantics. Only the largest record influences a `MAX`. A record's
 * influence on a `SUM` is the size of its contribution. That turns a million candidates
 * into the handful worth looking at.
 *
 * Obtain an instance from [[BigAsterisk.influence]].
 *
 * {{{
 * val ranked = BigAsterisk.influence(spark).influencers(df, "total > 100000")
 * ranked.head   // 1.0000  [o8,c2,99999]  (only the maximum influences)
 * }}}
 *
 * From *Influence-Based Provenance for Dataflow Applications with Taint Propagation*
 * (SoCC 2020).
 *
 * @group influence
 */
trait InfluenceSupport {

  /**
   * Ranks the records behind the results `faultyWhere` selects, most influential first.
   *
   * @param faultyWhere a SQL predicate over the query's output that is true for the
   *        results to explain
   * @param topK        how many records to return
   * @throws IllegalArgumentException if the predicate selects no result
   */
  def influencers(df: DataFrame, faultyWhere: String, topK: Int = 20): Seq[Influence]

  /**
   * The same ranking as [[influencers]], each entry as a JSON object with the record's
   * fields plus `__score` and `__reason`.
   *
   * Provided for language bindings that cannot marshal a Spark `Row` across the process
   * boundary — the PySpark front end reads this.
   */
  def influencersJson(df: DataFrame, faultyWhere: String, topK: Int = 20): Array[String]
}
