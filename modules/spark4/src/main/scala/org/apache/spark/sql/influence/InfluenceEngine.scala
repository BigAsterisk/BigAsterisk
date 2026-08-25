package org.apache.spark.sql.influence

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.classic.{ColumnConversions, ExpressionColumnNode, Dataset => ClassicDataset, SparkSession => ClassicSparkSession}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.udf.UdfAnalysis

import org.bigasterisk.api.{Influence, InfluenceSupport}

/**
 * Influence-based provenance for Spark SQL.
 *
 * ==Why provenance alone is not enough==
 * Every record of a group contributes to that group's aggregate, so ordinary provenance
 * for a wrong `MAX` over a million-row group returns a million records. Influence asks
 * *how much* each contributed, which for most aggregates is answerable from the
 * function's own semantics: only the largest record influences a `MAX`, and a record's
 * influence on a `SUM` is the size of its contribution relative to the rest.
 *
 * ==Approach==
 * Find the aggregation, take the rows entering it for the group behind the result in
 * question, evaluate the aggregate's argument on each, and apply the rule for that
 * function. No taint propagation and no re-execution: the semantics of `MAX` are known
 * in advance.
 *
 * ==Difference from the paper==
 * The original also propagates taint *inside* user-defined functions by source-to-source
 * transformation, which has no counterpart when the query is SQL rather than a Scala
 * program. Only the influence half of the technique is implemented here. See
 * `PROVENANCE.md`.
 */
class InfluenceEngine extends InfluenceSupport {

  override def influencers(df: DataFrame, faultyWhere: String, topK: Int = 20): Seq[Influence] = {
    require(topK >= 0, s"topK must not be negative, got $topK")

    val classic = df.sparkSession match {
      case c: ClassicSparkSession => c
      case other =>
        throw new UnsupportedOperationException(
          "Influence-based provenance needs a classic SparkSession; this one is " +
          s"${other.getClass.getName}. Spark Connect sessions are not supported: the " +
          "aggregation is located in the driver-side analyzed plan, which a Connect " +
          "client does not hold.")
    }

    val analyzed = df.queryExecution.analyzed
    val aggregate = analyzed.collectFirst { case a: Aggregate => a }

    aggregate match {
      case None =>
        // No many-to-one dependency: every record maps to at most one result, so
        // provenance is already exact and every witness is equally responsible.
        val selected = df.filter(expr(faultyWhere)).limit(topK).collect()
        require(selected.nonEmpty, s"no result matches '$faultyWhere'")
        val score = 1.0 / selected.length
        selected.toSeq.map(row =>
          Influence(row, score, "no aggregation in this query; provenance is already exact"))

      case Some(agg) =>
        val aggDf = ClassicDataset.ofRows(classic, agg)
        val faulty = aggDf.filter(expr(faultyWhere)).collect()
        require(faulty.nonEmpty,
          s"no aggregated result matches '$faultyWhere'; the predicate must name " +
          s"columns of the aggregation, which are: ${aggDf.schema.fieldNames.mkString(", ")}")

        val functions = InfluenceEngine.aggregateFunctionsOf(agg)
        val keyNames = agg.groupingExpressions.indices.map(i => s"__influence_key_$i")
        val argNames = functions.indices.map(i => s"__influence_arg_$i")

        // One pass over the aggregation's input, carrying the grouping key and each
        // aggregate's argument as extra columns. Everything after this is arithmetic on
        // collected rows: no re-execution, and no matching rows by content.
        val enriched = enrich(
          ClassicDataset.ofRows(classic, agg.child),
          agg.groupingExpressions.zip(keyNames) ++
            functions.zip(argNames).flatMap { case (f, n) => f.children.headOption.map(_ -> n) })
        val inputRows = enriched.collect().toSeq

        // Which columns of an input record can actually reach the result. Normally that
        // is every column the aggregate's argument mentions; where the argument passes
        // through a profiled Python UDF it is fewer, because an argument the function
        // never lets reach its return provably cannot change the answer.
        val tainted = functions
          .flatMap(_.children)
          .flatMap(UdfAnalysis.influencing)
          .map(_.name)
          .toSet

        val faultyKeys = keyedResults(agg, aggDf, keyNames, faulty)
        val scored = faultyKeys.flatMap { key =>
          val group = inputRows.filter(row => keyOf(row, keyNames) == key)
          influenceOf(functions, argNames, keyNames, group, tainted)
        }
        scored.sortBy(-_.score).take(topK)
    }
  }

  override def influencersJson(
      df: DataFrame, faultyWhere: String, topK: Int = 20): Array[String] = {
    val ranked = influencers(df, faultyWhere, topK)
    if (ranked.isEmpty) return Array.empty[String]
    import scala.jdk.CollectionConverters._
    val schema = ranked.head.row.schema
    val rowsJson = df.sparkSession
      .createDataFrame(ranked.map(_.row).asJava, schema).toJSON.collect()
    rowsJson.zip(ranked).map { case (json, influence) =>
      val body = json.trim
      val inner = body.substring(1, body.length - 1)
      val columns = influence.columns.toSeq.sorted.map(quote).mkString(",")
      val head = s""""__score":${influence.score},"__reason":${quote(influence.reason)},""" +
        s""""__columns":[$columns]"""
      if (inner.isEmpty) s"{$head}" else s"{$head,$inner}"
    }
  }

  /** Minimal JSON string escaping for the reason text. */
  private def quote(s: String): String =
    "\"" + s.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case c if c.isControl => f"\\u${c.toInt}%04x"
      case c    => c.toString
    } + "\""

  /** Adds each expression to `df` under the given name. */
  private def enrich(df: DataFrame, columns: Seq[(Expression, String)]): DataFrame =
    columns.foldLeft(df) { case (acc, (e, name)) => acc.withColumn(name, columnOf(e)) }

  private def keyOf(row: Row, keyNames: Seq[String]): Seq[Any] =
    keyNames.map(n => row.get(row.fieldIndex(n)))

  /**
   * The grouping keys of the faulty results.
   *
   * Matching is on the grouping expressions evaluated on both sides, which is what
   * defines a group; nothing else about the two plans has to line up. A global
   * aggregate has one group and an empty key.
   */
  private def keyedResults(
      agg: Aggregate,
      aggDf: DataFrame,
      keyNames: Seq[String],
      faulty: Array[Row]): Seq[Seq[Any]] = {
    if (agg.groupingExpressions.isEmpty) return Seq(Seq.empty)
    val resultColumns = aggDf.schema.fieldNames
    enrich(aggDf, agg.groupingExpressions.zip(keyNames))
      .collect()
      .filter(row => faulty.exists(f => resultColumns.forall(c =>
        f.get(f.fieldIndex(c)) == row.get(row.fieldIndex(c)))))
      .map(keyOf(_, keyNames))
      .distinct
      .toSeq
  }

  private def columnOf(e: Expression): Column = new Column(ExpressionColumnNode(e))

  /** Strips the bookkeeping columns, leaving the record as it entered the aggregation. */
  private def stripBookkeeping(row: Row, drop: Seq[String]): Row = {
    val keep = row.schema.fields.zipWithIndex.filterNot { case (f, _) => drop.contains(f.name) }
    new org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema(
      keep.map { case (_, i) => row.get(i) },
      org.apache.spark.sql.types.StructType(keep.map(_._1)))
  }

  /**
   * Scores one group's rows.
   *
   * A record is as influential as the most influential thing it does: when a query
   * aggregates several ways, a record that decides one of them matters, whatever it did
   * to the others.
   */
  private def influenceOf(
      functions: Seq[AggregateFunction],
      argNames: Seq[String],
      keyNames: Seq[String],
      rows: Seq[Row],
      tainted: Set[String]): Seq[Influence] = {

    if (rows.isEmpty) return Seq.empty
    val bookkeeping = keyNames ++ argNames
    val records = rows.map(stripBookkeeping(_, bookkeeping))

    val perFunction = functions.zip(argNames).flatMap { case (f, argName) =>
      val values = rows.map { row =>
        val i = row.schema.fieldNames.indexOf(argName)
        if (i < 0 || row.isNullAt(i)) None
        else row.get(i) match {
          case n: Number => Some(n.doubleValue())
          case _         => None
        }
      }
      InfluenceEngine.rule(f, rows, values)
    }

    if (perFunction.isEmpty) {
      val score = 1.0 / records.length
      return records.map(r => Influence(r, score,
        "aggregate semantics not modelled; every record weighted equally", tainted))
    }

    records.zipWithIndex.map { case (record, i) =>
      val best = perFunction.map(scores => scores(i)).maxBy(_._1)
      Influence(record, best._1, best._2, tainted)
    }
  }
}

object InfluenceEngine {

  /** The aggregate functions a plan computes. */
  def aggregateFunctionsOf(agg: Aggregate): Seq[AggregateFunction] =
    agg.aggregateExpressions.flatMap { e: NamedExpression =>
      e.collect { case a: AggregateExpression => a.aggregateFunction }
    }

  /**
   * The influence rule for one aggregate function, as `(score, reason)` per row, or
   * `None` when the function's semantics are not modelled.
   *
   * Scores within a group sum to 1, so they read as shares of responsibility.
   */
  def rule(
      f: AggregateFunction,
      rows: Seq[Row],
      values: Seq[Option[Double]]): Option[Seq[(Double, String)]] = {

    val defined = values.flatten
    if (rows.isEmpty) return None

    f match {
      case _: Max if defined.nonEmpty =>
        val peak = defined.max
        val winners = values.count(_.contains(peak))
        Some(values.map {
          case Some(v) if v == peak => (1.0 / winners, "only the maximum influences")
          case _                    => (0.0, "below the maximum; no influence")
        })

      case _: Min if defined.nonEmpty =>
        val trough = defined.min
        val winners = values.count(_.contains(trough))
        Some(values.map {
          case Some(v) if v == trough => (1.0 / winners, "only the minimum influences")
          case _                      => (0.0, "above the minimum; no influence")
        })

      case _: Sum | _: Average =>
        val magnitude = defined.map(math.abs).sum
        if (magnitude == 0.0) {
          val share = 1.0 / rows.length
          Some(rows.map(_ => (share, "every contribution is zero; weighted equally")))
        } else {
          Some(values.map {
            case Some(v) =>
              val share = math.abs(v) / magnitude
              (share, f"contribution ${share * 100}%.1f%% of the total magnitude")
            case None => (0.0, "null contributes nothing")
          })
        }

      case _: Count =>
        val share = 1.0 / rows.length
        Some(rows.map(_ => (share, "every record counts equally")))

      case _ => None
    }
  }
}
