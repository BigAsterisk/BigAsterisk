package org.apache.spark.sql.watchpoint

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.classic.{ColumnConversions, Dataset => ClassicDataset, SparkSession => ClassicSparkSession}

import org.bigasterisk.api.{Watchpoint, WatchpointSupport}

/**
 * On-demand watchpoints over the intermediate data of a Spark SQL query, on stock
 * Spark 4.
 *
 * The original BigDebug (ICSE 2016) shipped predicate bytecode to executors, hot-loaded
 * it through a custom class loader, and streamed matches back over RPC messages added
 * to Spark's `CoarseGrainedClusterMessages`. All three needed a forked Spark. Here the
 * guard is an ordinary Catalyst expression, which Spark already serializes with the
 * plan, and the matches travel by accumulator. See `PROVENANCE.md`.
 */
class Spark4Watchpoints extends WatchpointSupport {

  private val nextId = new AtomicLong(0L)
  private val registry = mutable.LinkedHashMap.empty[String, Spark4Watchpoint]

  override def watch(df: DataFrame, condition: Column, capacity: Int = 1000): Watchpoint = {
    require(capacity >= 0, s"watchpoint capacity must not be negative, got $capacity")

    val classic = df.sparkSession match {
      case c: ClassicSparkSession => c
      case other =>
        throw new UnsupportedOperationException(
          "Watchpoints need a classic SparkSession; this one is " +
          s"${other.getClass.getName}. Spark Connect sessions are not supported: the " +
          "guard is planned into the driver-side physical plan, which a Connect client " +
          "does not build.")
    }

    val id = s"bigasterisk-watchpoint-${nextId.incrementAndGet()}"
    val accumulator = new WatchpointAccumulator(capacity)
    // Registering names the accumulator in the Spark UI and is what makes Spark merge
    // executor updates back to the driver.
    classic.sparkContext.register(accumulator, id)

    // Attach to the *unanalyzed* plan so the analyzer resolves the guard against the
    // child's output, the same way it resolves a WHERE clause.
    val instrumented = ClassicDataset.ofRows(
      classic,
      WatchpointRelation(
        ColumnConversions.expression(condition), id, accumulator, df.queryExecution.logical))

    val wp = new Spark4Watchpoint(id, condition, capacity, accumulator, instrumented)
    registry.synchronized { registry(id) = wp }
    wp
  }

  override def active: Seq[Watchpoint] = registry.synchronized { registry.values.toSeq }

  override def clear(): Unit = registry.synchronized { registry.clear() }
}

/** A [[Watchpoint]] reading its observations out of a [[WatchpointAccumulator]]. */
private[watchpoint] class Spark4Watchpoint(
    override val id: String,
    private val guard: Column,
    override val capacity: Int,
    private val accumulator: WatchpointAccumulator,
    override val df: DataFrame) extends Watchpoint {

  // Spark 4 Columns are ColumnNode trees; the classic converter lowers one to a
  // Catalyst Expression, which is what the plan and the codegen path need.
  override def condition: String = ColumnConversions.expression(guard).sql

  override def hits: Long = accumulator.value.hits

  override def captured: Array[Row] = {
    val observed = accumulator.value.rows
    if (observed.isEmpty) {
      Array.empty[Row]
    } else {
      // The accumulator holds UnsafeRows; deserialize them against the watched schema.
      val toRow = RowEncoder.encoderFor(df.schema)
      val deserializer = org.apache.spark.sql.catalyst.encoders
        .ExpressionEncoder(toRow).resolveAndBind().createDeserializer()
      observed.map(deserializer).toArray
    }
  }

  override def capturedJson: Array[String] = {
    val rows = captured
    if (rows.isEmpty) {
      Array.empty[String]
    } else {
      import scala.jdk.CollectionConverters._
      df.sparkSession.createDataFrame(rows.toList.asJava, df.schema).toJSON.collect()
    }
  }

  override def reset(): Unit = accumulator.reset()

  override def toString: String =
    s"Watchpoint($id, condition=$condition, hits=$hits, captured=${captured.length}" +
      s"${if (truncated) s" of $hits" else ""})"
}
