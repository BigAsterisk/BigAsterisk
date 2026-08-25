package org.apache.spark.sql.bigdebug

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import org.bigasterisk.api.{Breakpoint, BreakpointSupport}

/**
 * Simulated breakpoints on stock Spark 4.
 *
 * ==Why nothing is inserted into the plan==
 * A breakpoint records where to look, not what was there. The state at a point in a
 * query is defined by the plan up to that point, and that plan is already in hand — so
 * regenerating the state is a matter of executing it, and setting a breakpoint costs
 * exactly nothing until someone asks.
 *
 * This is what makes the breakpoint *simulated*: the running job is never paused, never
 * instrumented, and never even told that a breakpoint exists.
 *
 * ==Materialization points==
 * Regeneration re-executes the prefix. [[Breakpoint.materialize]] pins the state so
 * that repeated inspection, and any resumed execution, start from there instead — the
 * "latest materialization point" the technique refers to.
 */
class BreakpointEngine extends BreakpointSupport {

  private val nextId = new AtomicLong(0L)
  private val registry = mutable.LinkedHashMap.empty[String, Spark4Breakpoint]

  override def breakpoint(df: DataFrame): Breakpoint = {
    val bp = new Spark4Breakpoint(s"bigasterisk-breakpoint-${nextId.incrementAndGet()}", df)
    registry.synchronized { registry(bp.id) = bp }
    bp
  }

  override def active: Seq[Breakpoint] = registry.synchronized { registry.values.toSeq }

  override def clear(): Unit = registry.synchronized {
    registry.values.foreach(_.release())
    registry.clear()
  }
}

/** A [[Breakpoint]] that regenerates its state from the query prefix. */
private[bigdebug] class Spark4Breakpoint(
    override val id: String,
    override val df: DataFrame) extends Breakpoint {

  @volatile private var pinned = false

  override def schema: StructType = df.schema

  override def state(limit: Int = 20): Array[Row] = {
    require(limit >= 0, s"limit must not be negative, got $limit")
    df.limit(limit).collect()
  }

  override def count(): Long = df.count()

  override def materialize(): Unit = synchronized {
    if (!pinned) {
      df.persist(StorageLevel.MEMORY_AND_DISK)
      df.count() // force it now, so the next inspection is served rather than recomputed
      pinned = true
    }
  }

  override def isMaterialized: Boolean = pinned

  override def release(): Unit = synchronized {
    if (pinned) {
      df.unpersist(blocking = false)
      pinned = false
    }
  }

  override def resumeWith(continue: DataFrame => DataFrame): DataFrame = continue(df)

  override def toString: String =
    s"Breakpoint($id, ${schema.fieldNames.mkString(", ")}" +
      s"${if (pinned) ", materialized" else ""})"
}
