package org.bigasterisk.api

import org.apache.spark.sql.{DataFrame, Row}

/**
 * One record and what it cost to produce.
 *
 * @param row    the record as it left the profiled point
 * @param nanos  time between this record and the previous one at that point — the work
 *               the upstream pipeline did for this record, not for the query as a whole
 *
 * @group perfdebug
 */
case class RecordCost(row: Row, nanos: Long) {
  /** [[nanos]] in milliseconds, for reporting. */
  def millis: Double = nanos / 1e6

  override def toString: String = f"$millis%.3f ms  $row"
}

/**
 * A profile of where a query's time went, record by record.
 *
 * @group perfdebug
 */
trait PerfProfile {

  /**
   * The instrumented DataFrame. Build the rest of the query on this, or nothing is
   * measured.
   */
  def df: DataFrame

  /**
   * The most expensive records seen, most expensive first, at most `topK` of them.
   *
   * These are what computation skew looks like: a handful of records that cost orders
   * of magnitude more than the rest.
   */
  def slowest: Seq[RecordCost]

  /** How many records passed the profiled point. */
  def records: Long

  /**
   * Whether costs can be attributed to individual records.
   *
   * False when a **batched** operator sits below the profiling point — a Python or
   * Arrow UDF evaluation, which computes a whole batch in one call to another process.
   * The totals stay exact, but the cost of the batch lands on whichever record happened
   * to trigger it rather than on the record that caused it, so [[slowest]] should not be
   * read as naming the expensive record.
   *
   * The totals can also *understate* in this case: a batch is computed before its first
   * output row appears, so its cost falls in the interval before that row — and the
   * first record of a task is never retained, because that interval spans pipeline
   * start-up rather than any record's work.
   *
   * Profile below the batched operator, or use a JVM-side expression, when record-level
   * attribution is what you need.
   */
  def recordLevel: Boolean

  /** Total time attributed to those records. */
  def totalNanos: Long

  /** Mean cost per record, in nanoseconds. */
  final def meanNanos: Double = if (records == 0L) 0.0 else totalNanos.toDouble / records

  /**
   * How far the most expensive record sits above the mean, as a multiple.
   *
   * A value near 1 means the cost is spread evenly. A large value is the signature of
   * computation skew, and the record it refers to is the one to look at.
   */
  final def skew: Double = {
    val mean = meanNanos
    if (mean == 0.0) 0.0 else slowest.headOption.map(_.nanos.toDouble / mean).getOrElse(0.0)
  }

  /**
   * The same records as [[slowest]], each as a JSON object with the record's fields
   * plus a `__nanos` entry giving its cost.
   *
   * Provided for language bindings that cannot marshal a Spark `Row` across the process
   * boundary — the PySpark front end reads this.
   */
  def slowestJson: Array[String]

  /** Discards what has been measured, so the profile can be reused for another run. */
  def reset(): Unit
}

/**
 * Performance debugging for computation skew: find the input records that cost
 * abnormally much to process.
 *
 * Data skew — one key having far more rows than others — is well understood and
 * visible in Spark's own metrics. *Computation* skew is not: a small number of records
 * can be far more expensive to process than the rest, and no per-task metric reveals
 * which ones. This measures cost at record granularity so the expensive records can be
 * named.
 *
 * Obtain an instance from [[BigAsterisk.perfdebug]].
 *
 * {{{
 * val profile = BigAsterisk.perfdebug(spark).profile(orders, topK = 10)
 * profile.df.groupBy("cid").sum("amount").collect()
 *
 * println(f"skew: \${profile.skew}%.1fx the mean")
 * profile.slowest.foreach(println)
 * }}}
 *
 * From *PerfDebug: Performance Debugging of Computation Skew in Dataflow Systems*
 * (SoCC 2019).
 *
 * @group perfdebug
 */
trait PerfDebugSupport {

  /**
   * Measures the cost of each record leaving `df`.
   *
   * Timing is taken inside Spark's generated code, so what is measured is the work the
   * upstream pipeline did for that record. Only the `topK` most expensive records are
   * retained; the rest are counted, so the mean and the skew ratio are exact.
   *
   * @param topK how many expensive records to keep. Keep it small: these are held on
   *        the driver.
   */
  def profile(df: DataFrame, topK: Int = 20): PerfProfile

  /** Every profile created in this session that has not been cleared. */
  def active: Seq[PerfProfile]

  /** Forgets every profile. Already-instrumented DataFrames keep working. */
  def clear(): Unit
}
