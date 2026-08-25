package org.apache.spark.sql.vega

import scala.collection.mutable

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.{Dataset => ClassicDataset, SparkSession => ClassicSparkSession}
import org.apache.spark.sql.desql.DeSqlEngine
import org.apache.spark.storage.StorageLevel

import org.bigasterisk.api.{VegaRun, VegaSupport}

/**
 * Incremental re-execution across successive revisions of a query.
 *
 * ==Approach==
 * A query decomposes into parts — the same parts [[DeSqlEngine]] exposes as steps. Each
 * part's plan is a complete sub-query, so its result can be materialized and handed to
 * a later revision that still contains it.
 *
 * Matching is on Catalyst's `canonicalized` form, which normalizes attribute ids and
 * other incidental differences, so two revisions that express the same sub-query match
 * even though they were parsed separately. That is deliberately the same basis Spark's
 * own `CacheManager` uses to decide whether a cached plan applies, which is what makes
 * substitution unnecessary: materializing a part is enough for the optimizer to route a
 * later revision through it.
 *
 * ==What is not implemented==
 * The paper's second optimization — rewriting the dataflow to push a code modification
 * as late as possible, so an edit near the sources does not invalidate the whole
 * prefix — is not implemented. Revisions that change an early operator will re-execute
 * from that operator down. See `PROVENANCE.md`.
 */
class VegaEngine(override val maxMaterialized: Int = VegaEngine.DefaultMaxMaterialized)
  extends VegaSupport {

  require(maxMaterialized >= 0, s"maxMaterialized must not be negative, got $maxMaterialized")

  /** Canonicalized plan -> the Dataset holding its materialized result. */
  private val store = mutable.LinkedHashMap.empty[LogicalPlan, VegaEngine.Entry]

  override def run(df: DataFrame): VegaRun = {
    val classic = df.sparkSession match {
      case c: ClassicSparkSession => c
      case other =>
        throw new UnsupportedOperationException(
          "Vega needs a classic SparkSession; this one is " +
          s"${other.getClass.getName}. Spark Connect sessions are not supported: reuse " +
          "is decided against the driver-side analyzed plan, which a Connect client " +
          "does not hold.")
    }

    val nodes = DeSqlEngine.stepNodes(df.queryExecution.analyzed)

    // Candidates, deepest first. Leaves are excluded: re-reading a source is what the
    // storage layer is for, and caching it would trade disk for memory with no saving
    // in work. The root is excluded because a revision, by definition, differs there.
    val candidates = nodes.zipWithIndex.collect {
      case (n, i) if n.plan.children.nonEmpty && i != nodes.length - 1 => n
    }

    val reused = mutable.ArrayBuffer.empty[String]
    val fresh = mutable.ArrayBuffer.empty[String]

    store.synchronized {
      candidates.foreach { node =>
        val key = node.plan.canonicalized
        if (store.contains(key)) {
          reused += describe(node)
        } else if (store.size < maxMaterialized) {
          // Persisting registers the plan with Spark's CacheManager; a later revision
          // containing the same sub-query is routed through it by the optimizer.
          val materialized = ClassicDataset.ofRows(classic, node.plan)
          materialized.persist(StorageLevel.MEMORY_AND_DISK)
          materialized.count() // force it now, so the next revision finds a warm cache
          store(key) = VegaEngine.Entry(describe(node), materialized)
          fresh += describe(node)
        }
      }
    }

    VegaEngine.Run(df, reused.toSeq, fresh.toSeq, nodes.length)
  }

  override def materialized: Seq[String] =
    store.synchronized { store.values.map(_.description).toSeq }

  override def clear(): Unit = store.synchronized {
    store.values.foreach(_.dataset.unpersist(blocking = false))
    store.clear()
  }

  private def describe(node: DeSqlEngine.StepNode): String =
    if (node.detail.isEmpty) node.operator else s"${node.operator} — ${node.detail}"
}

object VegaEngine {

  /**
   * How many query parts to hold materialized by default.
   *
   * Each one costs memory for the lifetime of the session, so the cap is deliberately
   * small: reuse across revisions comes overwhelmingly from the few parts nearest the
   * sources, and materializing further up buys little.
   */
  val DefaultMaxMaterialized: Int = 8

  private case class Entry(description: String, dataset: DataFrame)

  private case class Run(
      df: DataFrame,
      reused: Seq[String],
      materialized: Seq[String],
      steps: Int) extends VegaRun
}
