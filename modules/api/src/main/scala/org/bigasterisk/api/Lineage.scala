package org.bigasterisk.api

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * A position in a backward/forward walk over a captured lineage graph.
 *
 * A cursor is immutable: `goBack` and `goNext` return a new cursor rather than
 * mutating this one, so a walk can be branched and revisited.
 *
 * @group lineage
 */
trait TraceCursor {

  /** The lineage ids this cursor currently stands on. */
  def ids: Array[Long]

  /** True when the cursor has reached a source scan — `goBack` can go no further. */
  def atScan: Boolean

  /**
   * Steps one operator backward, toward the inputs.
   *
   * @param branch which input to follow when the operator has more than one
   *               (a join); `0` is the left/build side.
   */
  def goBack(branch: Int = 0): TraceCursor

  /** Steps one operator forward, toward the outputs. */
  def goNext(): TraceCursor

  /**
   * Materialises the records at this position.
   *
   * @param full when false, only the columns the capture retained; when true,
   *             the complete source rows, recovered by re-scanning the inputs.
   */
  def show(full: Boolean = false): Array[Row]

  /**
   * The same records as [[show]], each serialised as a JSON object.
   *
   * Provided for language bindings that cannot marshal a Spark `Row` across the
   * process boundary — the PySpark front end reads this.
   */
  def showJson(full: Boolean = false): Array[String]
}

/**
 * Record-level data provenance: run a query with capture on, then trace any output
 * record back to the exact input records that produced it.
 *
 * Obtain an instance from [[BigAsterisk.lineage]] rather than constructing one —
 * the implementation is chosen to match the running Spark version.
 *
 * @group lineage
 */
trait LineageSupport {

  /**
   * Turns capture on for subsequent queries in this session.
   *
   * The session must have been built with the binding's SQL extension registered;
   * [[BigAsterisk.configure]] does that. With capture off the extension is a no-op.
   */
  def enableCapture(spark: SparkSession): Unit

  /** Turns capture off. Already-captured lineage stays available until released. */
  def disableCapture(spark: SparkSession): Unit

  /** Runs `df` and returns each output row paired with its lineage id. */
  def collectWithLineage(df: DataFrame): Array[(Row, Long)]

  /** Opens a cursor standing on the given output ids of `df`. */
  def trace(df: DataFrame, outputIds: Seq[Long]): TraceCursor

  /** Walks all the way back and returns the source-record ids behind `outputIds`. */
  def backward(df: DataFrame, outputIds: Seq[Long]): Array[Long]

  /** Recovers the full source rows for ids returned by [[backward]]. */
  def showInputs(df: DataFrame, inputIds: Seq[Long]): Array[Row]

  /** Frees the lineage blocks held for `df`. Safe to call more than once. */
  def releaseLineage(df: DataFrame): Unit

  /** The lineage footprint retained for `df` as `(records, bytes)`. */
  def lineageSize(df: DataFrame): (Long, Long)

  /**
   * The lineage ids of `df`'s output rows, in the order `collect()` returns them.
   *
   * Lets a caller that already has the rows pair them with ids without collecting
   * twice; the PySpark front end uses this.
   */
  def resultIds(df: DataFrame): Array[Long]

  /**
   * [[trace]] for callers that cannot construct a Scala `Seq` — Java, and Py4J,
   * which marshals a Python list to `java.util.List`.
   */
  def traceJava(df: DataFrame, outputIds: java.util.List[java.lang.Number]): TraceCursor
}
