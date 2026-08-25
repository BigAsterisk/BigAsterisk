package org.bigasterisk.api

import org.apache.spark.sql.{DataFrame, Row}

/**
 * The record a query was processing when it failed.
 *
 * @param row          the record in flight at the moment of failure
 * @param partitionId  the partition whose task failed
 * @param recordIndex  how many records that task had seen, so the record can be located
 *                     in the input by position as well as by value
 * @param error        the exception's type and message
 *
 * @group crashculprit
 */
case class CulpritRecord(row: Row, partitionId: Int, recordIndex: Long, error: String) {
  override def toString: String =
    s"partition $partitionId, record $recordIndex: $row\n  $error"
}

/**
 * A guard that remembers what a query was working on when it died.
 *
 * @group crashculprit
 */
trait CrashCulprit {

  /** Identifier, unique within the session. */
  def id: String

  /**
   * The instrumented DataFrame. Run this rather than the one passed to
   * [[CrashCulpritSupport.guard]], or nothing is recorded.
   */
  def df: DataFrame

  /**
   * The record in flight when the query failed, if it did.
   *
   * `None` while the query has not failed. Note that a task failing is not the same as
   * the query failing: Spark retries, and a record that kills every attempt is the one
   * reported here.
   *
   * ==Batched Python UDFs==
   * This does not work across a batched Python or Arrow UDF. Such a UDF takes a whole
   * batch to another process before any of it can fail, so by the time the exception
   * comes back the guard has already emitted every row of that batch and the record it
   * remembers is the last of the batch, not the one that failed. Guarding *above* the
   * UDF does not help either: the guard would then never see the failing batch at all.
   *
   * Unlike [[PerfProfile.recordLevel]], this cannot be detected and reported. A profile
   * is compromised by a batched operator *below* it, which is part of the plan it holds
   * and can be inspected; a guard is compromised by one *above* it, which is whatever
   * the caller goes on to build and is not knowable from the guard.
   *
   * Express the failing computation in SQL when you need the record named.
   */
  def culprit: Option[CulpritRecord]

  /**
   * [[culprit]] as a JSON object with the record's fields plus `__partitionId`,
   * `__recordIndex` and `__error`, or `null` when the query has not failed.
   *
   * Provided for language bindings that cannot marshal a Spark `Row` across the process
   * boundary — the PySpark front end reads this.
   */
  def culpritJson: String

  /** Forgets what was recorded, so the guard can be reused for another run. */
  def reset(): Unit
}

/**
 * Crash-culprit determination: when a query dies on bad data, say which record killed
 * it.
 *
 * A failing Spark job reports a stack trace and a task id. Neither tells you which of
 * the billion records being processed was the one the code could not handle, and the
 * usual recourse — bisecting the input by hand — is exactly the work this removes.
 *
 * This is BigDebug's crash-culprit primitive (ICSE 2016). Obtain an instance from
 * [[BigAsterisk.crashCulprit]].
 *
 * {{{
 * val guard = BigAsterisk.crashCulprit(spark).guard(orders)
 * try guard.df.selectExpr("CAST(amount AS INT) / 0").collect()
 * catch { case _: Exception => println(guard.culprit.get) }
 * }}}
 *
 * @group crashculprit
 */
trait CrashCulpritSupport {

  /**
   * Watches the records flowing out of `df`, so that if the query fails the last record
   * seen can be reported.
   *
   * The cost is writing each record into a reused buffer — no allocation per record,
   * and no data moves unless something actually fails.
   */
  def guard(df: DataFrame): CrashCulprit

  /** Every guard created in this session that has not been cleared. */
  def active: Seq[CrashCulprit]

  /** Forgets every guard. Already-instrumented DataFrames keep working. */
  def clear(): Unit
}
