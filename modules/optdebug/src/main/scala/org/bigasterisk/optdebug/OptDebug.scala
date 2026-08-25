package org.bigasterisk.optdebug

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.expr

import org.bigasterisk.api.{BigAsterisk, SuspiciousOperation, Suspiciousness}

/**
 * The result of localising a fault to the operations of a query.
 *
 * @param ranked          operations, most suspicious first
 * @param failingOutputs  output rows the oracle rejected
 * @param failingWitnesses source records behind the failing outputs
 * @param passingWitnesses source records behind the accepted outputs
 * @param formula         the scoring formula used
 */
case class OptDebugResult(
    ranked: Seq[SuspiciousOperation],
    failingOutputs: Array[Row],
    failingWitnesses: Long,
    passingWitnesses: Long,
    formula: String) {

  /** The most suspicious operation, if the query had any to score. */
  def prime: Option[SuspiciousOperation] = ranked.headOption

  override def toString: String =
    s"OptDebugResult($formula, ${failingOutputs.length} failing outputs, " +
      s"$failingWitnesses failing / $passingWitnesses passing witnesses)\n" +
      ranked.map("  " + _).mkString("\n")
}

/**
 * Fault-inducing operation isolation: given a query and a test oracle, rank the
 * query's operations by how responsible each looks for the wrong result.
 *
 * Data provenance answers "which records produced this?". It says nothing about
 * *which part of the query* was at fault. OptDebug (SoCC 2021) closes that gap by
 * scoring operations the way spectrum-based fault localisation scores lines of code:
 * an operation that most failing records passed through, and few passing records did,
 * is the one to look at.
 *
 * {{{
 * val result = OptDebug.localize(
 *   spark,
 *   spark.sql("SELECT cid, SUM(amount) AS total FROM orders GROUP BY cid"),
 *   row => row.getLong(1) < 0)          // a negative total is wrong
 *
 * result.ranked.foreach(println)
 * // 0.816  [1] Filter — (amount > 0)  (failing=3, passing=0)
 * }}}
 *
 * ==How the spectra are gathered==
 * Each operation of the query is itself a complete sub-query (the same decomposition
 * [[org.bigasterisk.api.DeSqlSupport]] exposes). Running one with provenance capture
 * on tells us which source records reach its output. Intersecting that set with the
 * records behind the failing outputs, and with those behind the passing outputs, gives
 * the operation's spectrum directly — no instrumentation of the operation itself, and
 * no re-execution with modified inputs.
 *
 * This module depends only on `bigasterisk-api`: the scoring is arithmetic and the
 * provenance comes through the binding, so nothing here is tied to a Spark version.
 */
object OptDebug {

  /**
   * Ranks the operations of `df` by how responsible each looks for the outputs that
   * `oracle` rejects.
   *
   * @param oracle  returns true for an output row that is *wrong*
   * @param formula the suspiciousness formula; Tarantula by default, because it stays
   *        neutral for operations that touch every record
   * @throws IllegalArgumentException if the oracle rejects nothing, since there is then
   *         no fault to localise
   */
  def localize(
      spark: SparkSession,
      df: DataFrame,
      oracle: Row => Boolean,
      formula: Suspiciousness = Suspiciousness.Tarantula): OptDebugResult = {

    val lineage = BigAsterisk.lineage(spark)
    val desql = BigAsterisk.desql(spark)

    lineage.enableCapture(spark)
    try {
      // 1. Split the output into what the oracle rejects and what it accepts, and trace
      //    each group back to the source records behind it.
      val outputs = lineage.collectWithLineage(df)
      val (failing, passing) = outputs.partition { case (row, _) => oracle(row) }
      require(failing.nonEmpty,
        "the oracle accepted every output row, so there is no fault to localise")

      val failingWitnesses = witnessesOf(lineage, df, failing.map(_._2))
      val passingWitnesses = witnessesOf(lineage, df, passing.map(_._2))
      lineage.releaseLineage(df)

      // 2. Score each operation by the source records that reach it. Source scans are
      //    skipped: every record reaches them, so they carry no signal.
      //
      //    Two granularities are scored. The operator itself discriminates when it
      //    drops records — a join, a filter, a grouping. Its *branches* discriminate
      //    when it does not: every record flows through a projection, but only some
      //    take a given arm of its CASE WHEN, and that is where a faulty operation
      //    inside an expression shows up.
      val totalFailing = failingWitnesses.rows.size.toLong
      val totalPassing = passingWitnesses.rows.size.toLong

      def scoreOf(reached: Witnesses): Option[(Long, Long, Double)] = {
        // An operation no record reaches — a branch nothing takes — is legitimately
        // unsuspicious, and saying so is informative. It is not the same thing as an
        // operation we could not match, which is dropped below.
        if (reached.rows.isEmpty) return Some((0L, 0L, formula.score(0L, 0L, totalFailing, totalPassing)))

        val common = reached.columns
          .intersect(failingWitnesses.columns)
          .intersect(if (passingWitnesses.rows.isEmpty) reached.columns
                     else passingWitnesses.columns)
        if (common.isEmpty) {
          // nothing to match on; scoring this operation would be a guess
          None
        } else {
          val failingKeys = failingWitnesses.keys(common)
          val passingKeys = passingWitnesses.keys(common)
          val reachedKeys = reached.keys(common)
          val ef = reachedKeys.count(failingKeys.contains).toLong
          val ep = reachedKeys.count(passingKeys.contains).toLong
          Some((ef, ep, formula.score(ef, ep, totalFailing, totalPassing)))
        }
      }

      val steps = desql.decompose(df)
      val scored = steps.filter(_.childIds.nonEmpty).flatMap { step =>
        val operatorRow = reachedBy(lineage, step.data).flatMap(scoreOf).map {
          case (ef, ep, s) =>
            SuspiciousOperation(step.id, step.operator, step.detail, None, ef, ep, s)
        }
        val branchRows = step.branches.flatMap { branch =>
          reachedBy(lineage, branch.data).flatMap(scoreOf).map {
            case (ef, ep, s) =>
              SuspiciousOperation(
                step.id, step.operator, step.detail, Some(branch.description), ef, ep, s)
          }
        }
        operatorRow.toSeq ++ branchRows
      }

      OptDebugResult(
        // most suspicious first; ties break toward the finer-grained operation, then
        // toward the earlier step, so the ranking is deterministic
        ranked = scored.sortBy(op => (-op.score, !op.isBranch, op.stepId)),
        failingOutputs = failing.map(_._1),
        failingWitnesses = totalFailing,
        passingWitnesses = totalPassing,
        formula = formula.name)
    } finally {
      lineage.disableCapture(spark)
    }
  }

  /** The column the SQL-predicate form of [[localize]] adds to carry the verdict. */
  private val VerdictColumn = "__bigasterisk_faulty"

  /**
   * Ranks the operations of `df`, with the oracle given as a SQL predicate over the
   * query's output columns rather than as a function.
   *
   * Equivalent to the function form, and the one to use from a language binding: a
   * predicate crosses a process boundary, a closure does not.
   *
   * {{{
   * OptDebug.localize(spark, df, "total < 0")
   * }}}
   *
   * The predicate is evaluated as an extra column of the query, so it sees the output
   * schema by name and is computed by Spark rather than on the driver.
   */
  def localize(
      spark: SparkSession,
      df: DataFrame,
      faultyWhere: String,
      formula: Suspiciousness): OptDebugResult = {
    val augmented = df.withColumn(VerdictColumn, expr(faultyWhere))
    val verdictIndex = augmented.schema.fieldIndex(VerdictColumn)
    val result = localize(
      spark,
      augmented,
      row => !row.isNullAt(verdictIndex) && row.getBoolean(verdictIndex),
      formula)
    // hide the bookkeeping column from the reported outputs
    result.copy(failingOutputs = result.failingOutputs.map(dropVerdict))
  }

  /** As above, with the default formula. */
  def localize(spark: SparkSession, df: DataFrame, faultyWhere: String): OptDebugResult =
    localize(spark, df, faultyWhere, Suspiciousness.Tarantula)

  /** Named formulas, for callers that cannot name a Scala object. */
  def formulaByName(name: String): Suspiciousness =
    Suspiciousness.all.find(_.name.equalsIgnoreCase(name)).getOrElse {
      throw new IllegalArgumentException(
        s"unknown suspiciousness formula '$name'; expected one of " +
          Suspiciousness.all.map(_.name).mkString(", "))
    }

  private def dropVerdict(row: Row): Row = {
    val keep = row.schema.fieldNames.zipWithIndex.filter(_._1 != VerdictColumn)
    val values = keep.map { case (_, i) => row.get(i) }
    val schema = org.apache.spark.sql.types.StructType(
      row.schema.fields.filter(_.name != VerdictColumn))
    new org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema(values, schema)
  }

  /**
   * A set of source records, kept as rows rather than lineage ids.
   *
   * Lineage ids are positions — `(partition, index)` — assigned per execution, so the
   * id of a record in one sub-query has nothing to do with its id in another. Every
   * operation here is executed as its own sub-query, so the populations have to be
   * matched on record *content*.
   *
   * Column pruning also means two sub-queries can recover different columns of the same
   * source, so matching uses only the columns both sides expose. Two source rows that
   * agree on all of those count as the same record; genuinely duplicated source rows
   * therefore conflate, which inflates neither population relative to the other.
   */
  private case class Witnesses(columns: Seq[String], rows: Seq[Row]) {
    def keys(on: Seq[String]): Set[Seq[Any]] =
      rows.map(row => on.map(c => row.get(row.fieldIndex(c)))).toSet
  }

  private object Witnesses {
    val empty: Witnesses = Witnesses(Seq.empty, Seq.empty)

    def of(rows: Array[Row]): Witnesses =
      if (rows.isEmpty) empty
      else Witnesses(rows.head.schema.fieldNames.toSeq, rows.toSeq)
  }

  /** The source records behind a set of output ids. */
  private def witnessesOf(
      lineage: org.bigasterisk.api.LineageSupport,
      df: DataFrame,
      outputIds: Array[Long]): Witnesses =
    if (outputIds.isEmpty) Witnesses.empty
    else {
      val sourceIds = lineage.backward(df, outputIds.toSeq)
      if (sourceIds.isEmpty) Witnesses.empty
      else Witnesses.of(lineage.showInputs(df, sourceIds.toSeq))
    }

  /**
   * The source records reaching `stepDf`'s output, or `None` when the operation cannot
   * be captured on its own.
   *
   * An operation outside the capture engine's verified set aborts rather than
   * returning wrong lineage, and a step of a correlated subquery cannot execute
   * standalone at all. Either way the operation contributes no spectrum and is dropped
   * from the ranking rather than scored on a guess.
   */
  private def reachedBy(
      lineage: org.bigasterisk.api.LineageSupport,
      stepDf: => DataFrame): Option[Witnesses] =
    try {
      val df = stepDf
      val ids = lineage.collectWithLineage(df).map(_._2)
      val reached = witnessesOf(lineage, df, ids)
      lineage.releaseLineage(df)
      Some(reached)
    } catch {
      case _: UnsupportedOperationException => None
      case _: RuntimeException => None
    }
}
