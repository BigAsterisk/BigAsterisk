package org.apache.spark.sql.bigdebug

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.classic.{Dataset => ClassicDataset, SparkSession => ClassicSparkSession}

import org.bigasterisk.api.{CrashCulprit, CrashCulpritSupport, CulpritRecord}

/**
 * Crash-culprit determination on stock Spark 4.
 *
 * The original BigDebug caught the record in flight through a patched task iterator and
 * shipped it over an RPC message added to Spark's `CoarseGrainedClusterMessages`. Here
 * the operator is injected through `spark.sql.extensions` and the record travels back on
 * an accumulator registered to survive task failure. See `PROVENANCE.md`.
 */
class CrashCulpritEngine extends CrashCulpritSupport {

  private val nextId = new AtomicLong(0L)
  private val registry = mutable.LinkedHashMap.empty[String, Spark4CrashCulprit]

  override def guard(df: DataFrame): CrashCulprit = {
    val classic = df.sparkSession match {
      case c: ClassicSparkSession => c
      case other =>
        throw new UnsupportedOperationException(
          "Crash-culprit guards need a classic SparkSession; this one is " +
          s"${other.getClass.getName}. Spark Connect sessions are not supported: the " +
          "guard is planned into the driver-side physical plan, which a Connect client " +
          "does not build.")
    }

    val id = s"bigasterisk-culprit-${nextId.incrementAndGet()}"
    val accumulator = new CulpritAccumulator
    // countFailedValues = true: the whole point is to hear from the task that died.
    accumulator.register(classic.sparkContext, Some(id), countFailedValues = true)

    val instrumented = ClassicDataset.ofRows(
      classic, CrashCulpritRelation(id, accumulator, df.queryExecution.logical))

    val guarded = new Spark4CrashCulprit(id, accumulator, instrumented)
    registry.synchronized { registry(id) = guarded }
    guarded
  }

  override def active: Seq[CrashCulprit] = registry.synchronized { registry.values.toSeq }

  override def clear(): Unit = registry.synchronized { registry.clear() }
}

/** A [[CrashCulprit]] reading its report out of a [[CulpritAccumulator]]. */
private[bigdebug] class Spark4CrashCulprit(
    override val id: String,
    private val accumulator: CulpritAccumulator,
    override val df: DataFrame) extends CrashCulprit {

  override def culprit: Option[CulpritRecord] = accumulator.value.map { report =>
    val deserializer = ExpressionEncoder(RowEncoder.encoderFor(df.schema))
      .resolveAndBind().createDeserializer()
    CulpritRecord(deserializer(report.row), report.partitionId, report.recordIndex, report.error)
  }

  override def culpritJson: String = culprit.map { c =>
    import scala.jdk.CollectionConverters._
    val body = df.sparkSession
      .createDataFrame(List(c.row).asJava, df.schema).toJSON.collect().head.trim
    val inner = body.substring(1, body.length - 1)
    val head =
      s""""__partitionId":${c.partitionId},"__recordIndex":${c.recordIndex},""" +
        s""""__error":${quote(c.error)}"""
    if (inner.isEmpty) s"{$head}" else s"{$head,$inner}"
  }.orNull

  /** Minimal JSON string escaping for the error text. */
  private def quote(s: String): String =
    "\"" + s.flatMap {
      case '"'              => "\\\""
      case '\\'             => "\\\\"
      case '\n'             => "\\n"
      case '\r'             => "\\r"
      case '\t'             => "\\t"
      case c if c.isControl => f"\\u${c.toInt}%04x"
      case c                => c.toString
    } + "\""

  override def reset(): Unit = accumulator.reset()

  override def toString: String =
    culprit.map(c => s"CrashCulprit($id, $c)").getOrElse(s"CrashCulprit($id, no failure)")
}
