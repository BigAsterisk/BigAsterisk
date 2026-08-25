package org.apache.spark.sql.perfdebug

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.classic.{Dataset => ClassicDataset, SparkSession => ClassicSparkSession}

import org.bigasterisk.api.{PerfDebugSupport, PerfProfile, RecordCost}

/**
 * Performance debugging for computation skew, on stock Spark 4.
 *
 * The original PerfDebug propagated a latency value alongside every record through a
 * forked Spark's lineage machinery, and stored it in Apache Ignite. Here the timing is
 * taken inside Spark's generated code and travels back by accumulator, and there is no
 * external store. See `PROVENANCE.md`.
 */
class PerfDebugEngine extends PerfDebugSupport {

  private val nextId = new AtomicLong(0L)
  private val registry = mutable.LinkedHashMap.empty[String, Spark4PerfProfile]

  override def profile(df: DataFrame, topK: Int = 20): PerfProfile = {
    require(topK >= 0, s"topK must not be negative, got $topK")

    val classic = df.sparkSession match {
      case c: ClassicSparkSession => c
      case other =>
        throw new UnsupportedOperationException(
          "Profiling needs a classic SparkSession; this one is " +
          s"${other.getClass.getName}. Spark Connect sessions are not supported: the " +
          "timing operator is planned into the driver-side physical plan, which a " +
          "Connect client does not build.")
    }

    val id = s"bigasterisk-profile-${nextId.incrementAndGet()}"
    val accumulator = new LatencyAccumulator(topK)
    classic.sparkContext.register(accumulator, id)

    val instrumented = ClassicDataset.ofRows(
      classic, LatencyRelation(id, accumulator, df.queryExecution.logical))

    val p = new Spark4PerfProfile(id, topK, accumulator, instrumented)
    registry.synchronized { registry(id) = p }
    p
  }

  override def active: Seq[PerfProfile] = registry.synchronized { registry.values.toSeq }

  override def clear(): Unit = registry.synchronized { registry.clear() }
}

/** A [[PerfProfile]] reading its measurements out of a [[LatencyAccumulator]]. */
private[perfdebug] class Spark4PerfProfile(
    val id: String,
    val topK: Int,
    private val accumulator: LatencyAccumulator,
    override val df: DataFrame) extends PerfProfile {

  override def records: Long = accumulator.value.records

  /**
   * A batched Python/Arrow UDF evaluation below the profiling point makes per-record
   * attribution meaningless, because the whole batch is computed in one call to another
   * process. Detected from the planned physical plan rather than assumed.
   */
  override lazy val recordLevel: Boolean =
    !df.queryExecution.executedPlan.exists {
      case _: org.apache.spark.sql.execution.python.BatchEvalPythonExec => true
      case _: org.apache.spark.sql.execution.python.ArrowEvalPythonExec => true
      case _ => false
    }

  override def totalNanos: Long = accumulator.value.totalNanos

  override def slowest: Seq[RecordCost] = {
    val observed = accumulator.value.slowest
    if (observed.isEmpty) {
      Seq.empty
    } else {
      val deserializer = ExpressionEncoder(RowEncoder.encoderFor(df.schema))
        .resolveAndBind().createDeserializer()
      observed.map { case (nanos, row) => RecordCost(deserializer(row), nanos) }
    }
  }

  override def slowestJson: Array[String] = {
    val costs = slowest
    if (costs.isEmpty) {
      Array.empty[String]
    } else {
      import scala.jdk.CollectionConverters._
      val rowsJson = df.sparkSession
        .createDataFrame(costs.map(_.row).asJava, df.schema).toJSON.collect()
      // splice the cost in beside the record's own fields
      rowsJson.zip(costs).map { case (json, cost) =>
        val body = json.trim
        val inner = body.substring(1, body.length - 1)
        if (inner.isEmpty) s"""{"__nanos":${cost.nanos}}"""
        else s"""{"__nanos":${cost.nanos},$inner}"""
      }
    }
  }

  override def reset(): Unit = accumulator.reset()

  override def toString: String =
    f"PerfProfile($id, records=$records, mean=${meanNanos / 1e6}%.3f ms, skew=$skew%.1fx)"
}
