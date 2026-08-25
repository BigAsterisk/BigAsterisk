package org.bigasterisk.api

import java.util.ServiceLoader
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession

/**
 * Entry point to the BigAsterisk platform.
 *
 * BigAsterisk attaches to a stock Apache Spark installation as a library — there is
 * no forked Spark and no patched jar. Build a session through [[configure]] so the
 * binding for your Spark version installs itself, then reach the tools through it:
 *
 * {{{
 * import org.bigasterisk.api.BigAsterisk
 *
 * val spark = BigAsterisk.configure(SparkSession.builder().master("local[*]")).getOrCreate()
 * val lineage = BigAsterisk.lineage(spark)
 * lineage.enableCapture(spark)
 *
 * val df = spark.sql("SELECT category, SUM(amount) AS total FROM sales GROUP BY category")
 * val rows = lineage.collectWithLineage(df)
 * lineage.trace(df, Seq(rows.head._2)).goBack().show().foreach(println)
 * lineage.releaseLineage(df)
 * }}}
 *
 * @groupname entry Entry points
 * @groupname spi Service provider interface
 * @groupname lineage Data provenance
 * @groupname desql Step-through SQL debugging
 * @groupname watchpoint Watchpoints
 * @groupname crashculprit Crash-culprit determination
 * @groupname vega Incremental re-execution
 * @groupname perfdebug Performance debugging
 * @groupname influence Influence-based provenance
 * @groupname fuzz Fuzz testing
 * @groupname testgen Symbolic test generation
 */
object BigAsterisk {

  /**
   * Every [[SparkBinding]] on the classpath, in discovery order.
   *
   * `ServiceLoader` constructs a fresh instance of each provider per call, so this is a
   * discovery mechanism, not a way to reach a binding's state. Use [[bindingFor]] or
   * [[binding]], which return the one stable instance per Spark version.
   *
   * @group spi
   */
  def bindings: Seq[SparkBinding] =
    ServiceLoader.load(classOf[SparkBinding], getClass.getClassLoader).asScala.toSeq

  /**
   * One binding instance per Spark version, for the lifetime of the JVM.
   *
   * Bindings hold state that must not be recreated underneath a caller — the registry
   * of live watchpoints, for one — so resolution has to be stable. A failed lookup is
   * not cached, which leaves the door open for a binding jar added to a live classpath
   * (a notebook `%AddJar`, a REPL `:require`) to be found on a later call.
   */
  private val resolved = new ConcurrentHashMap[String, SparkBinding]()

  /**
   * The binding that supports `sparkVersion`.
   *
   * @throws IllegalStateException if none matches, listing what was found so the
   *         mismatch is obvious from the message alone.
   * @group spi
   */
  def bindingFor(sparkVersion: String): SparkBinding =
    resolved.computeIfAbsent(sparkVersion, v => select(bindings, v))

  /**
   * Chooses the binding for `sparkVersion` out of `candidates`.
   *
   * Separated from [[bindings]] so the selection rule can be tested without going
   * through `ServiceLoader`. When several bindings match, the one with the highest
   * lower bound wins, so a binding published for a specific release takes precedence
   * over a broader fallback.
   *
   * @throws IllegalStateException if none matches, listing what was found.
   */
  private[api] def select(candidates: Seq[SparkBinding], sparkVersion: String): SparkBinding = {
    if (candidates.isEmpty) {
      throw new IllegalStateException(
        s"No BigAsterisk Spark binding found on the classpath (running Spark $sparkVersion). " +
        "Add the binding jar for your Spark version; for Spark 4.x that is bigasterisk-spark4.")
    }
    val v = Version.parse(sparkVersion)
    val matching = candidates.filter(_.versionRange.contains(v))
    if (matching.isEmpty) {
      val available = candidates.map(b => s"  ${b.name} supports ${b.sparkVersions}").mkString("\n")
      throw new IllegalStateException(
        s"No BigAsterisk binding supports Spark $sparkVersion.\nAvailable bindings:\n$available")
    }
    matching.maxBy(_.versionRange.lower.getOrElse(Version(Seq(0), None)))
  }

  /**
   * The binding for `spark`'s version, after checking it can attach to this session.
   *
   * @group spi
   */
  def binding(spark: SparkSession): SparkBinding = {
    val b = bindingFor(spark.version)
    b.validate(spark)
    b
  }

  /**
   * Applies the binding's [[SparkBinding.requiredConf]] to `builder`.
   *
   * Spark reads `spark.sql.extensions` when the session is built, so this must run
   * before `getOrCreate()`. Existing values of `spark.sql.extensions` are preserved:
   * the binding's extension is appended to the comma-separated list rather than
   * replacing whatever the caller already configured.
   *
   * @param sparkVersion the Spark version to bind against. Defaults to the version
   *        of the `spark-core` on this classpath.
   * @group entry
   */
  def configure(
      builder: SparkSession.Builder,
      sparkVersion: String = org.apache.spark.SPARK_VERSION): SparkSession.Builder = {
    val b = bindingFor(sparkVersion)
    b.requiredConf.foreach {
      case (k @ "spark.sql.extensions", v) => builder.config(k, appendExtension(v))
      case (k, v)                          => builder.config(k, v)
    }
    builder
  }

  /**
   * Merges `extension` into any `spark.sql.extensions` already set on the JVM's
   * Spark configuration, preserving order and dropping duplicates.
   */
  private def appendExtension(extension: String): String = {
    val existing = Option(System.getProperty("spark.sql.extensions"))
      .toSeq
      .flatMap(_.split(','))
      .map(_.trim)
      .filter(_.nonEmpty)
    (existing ++ extension.split(',').map(_.trim).filter(_.nonEmpty)).distinct.mkString(",")
  }

  /**
   * Record-level data provenance for `spark`.
   *
   * @group lineage
   */
  def lineage(spark: SparkSession): LineageSupport = binding(spark).lineage

  /**
   * Step-through SQL debugging for `spark`: decompose a query and inspect the
   * intermediate data at each part.
   *
   * @group desql
   */
  def desql(spark: SparkSession): DeSqlSupport = binding(spark).desql

  /**
   * On-demand watchpoints for `spark`: guard the intermediate data of a query and see
   * which records match, without collecting the intermediate dataset.
   *
   * @group watchpoint
   */
  def watchpoints(spark: SparkSession): WatchpointSupport = binding(spark).watchpoints

  /**
   * Crash-culprit determination for `spark`: when a query dies on bad data, which
   * record killed it.
   *
   * @group crashculprit
   */
  def crashCulprit(spark: SparkSession): CrashCulpritSupport = binding(spark).crashCulprit

  /**
   * Incremental re-execution for `spark`: successive revisions of a query start from
   * the deepest point they still share, rather than from nothing.
   *
   * @group vega
   */
  def vega(spark: SparkSession): VegaSupport = binding(spark).vega

  /**
   * Computation-skew profiling for `spark`: which records cost abnormally much to
   * process.
   *
   * @group perfdebug
   */
  def perfdebug(spark: SparkSession): PerfDebugSupport = binding(spark).perfdebug

  /**
   * Influence-based provenance for `spark`: of the records behind a result, which ones
   * actually mattered.
   *
   * @group influence
   */
  def influence(spark: SparkSession): InfluenceSupport = binding(spark).influence

  /**
   * Fuzz testing for `spark`: generate inputs for a query and see what breaks.
   *
   * @group fuzz
   */
  def fuzz(spark: SparkSession): FuzzSupport = binding(spark).fuzz

  /**
   * Systematic test-input generation for `spark`: solve the query's own conditions and
   * construct an input per path through them.
   *
   * @group testgen
   */
  def testgen(spark: SparkSession): TestGenSupport = binding(spark).testgen
}
