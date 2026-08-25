package org.bigasterisk.api

import org.apache.spark.sql.SparkSession

/**
 * The service interface that binds BigAsterisk's tools to one family of Spark
 * releases.
 *
 * BigAsterisk never patches or forks Spark. Everything that depends on a particular
 * Spark version — physical-plan rules, codegen taps, executor-side storage — lives
 * behind this interface in its own module; the tools compile against
 * [[LineageSupport]] and the rest of `org.bigasterisk.api` only.
 *
 * ==Adding support for a new Spark release==
 *   1. Create `modules/sparkN` implementing this trait.
 *   2. Declare the releases it handles in [[sparkVersions]], e.g. `"[5.0.0,6.0.0)"`.
 *   3. Register it by adding the implementation's fully qualified name to
 *      `src/main/resources/META-INF/services/org.bigasterisk.api.SparkBinding`.
 *
 * [[BigAsterisk]] then selects it automatically at runtime. No tool module changes.
 *
 * @group spi
 */
trait SparkBinding {

  /** A short human-readable name, e.g. `"spark4"`. Used in diagnostics. */
  def name: String

  /**
   * The Spark releases this binding supports, as a Maven-style range.
   *
   * @see [[VersionRange.parse]]
   */
  def sparkVersions: String

  /** Parsed form of [[sparkVersions]]. */
  final def versionRange: VersionRange = VersionRange.parse(sparkVersions)

  /**
   * Session configuration this binding needs in order to install itself —
   * typically `spark.sql.extensions`. Applied by [[BigAsterisk.configure]]
   * *before* the session is created, since Spark reads these at build time.
   */
  def requiredConf: Map[String, String]

  /** Record-level data provenance for this Spark version. */
  def lineage: LineageSupport

  /** Step-through SQL debugging for this Spark version. */
  def desql: DeSqlSupport

  /** On-demand watchpoints over intermediate data for this Spark version. */
  def watchpoints: WatchpointSupport

  /** Incremental re-execution across query revisions for this Spark version. */
  def vega: VegaSupport

  /** Computation-skew profiling for this Spark version. */
  def perfdebug: PerfDebugSupport

  /** Influence-based provenance for this Spark version. */
  def influence: InfluenceSupport

  /** Fuzz testing for this Spark version. */
  def fuzz: FuzzSupport

  /** Systematic test-input generation for this Spark version. */
  def testgen: TestGenSupport

  /**
   * Verifies the binding can actually attach to `spark`, throwing a descriptive
   * exception if not — for example when the session was built without
   * [[requiredConf]] applied.
   */
  def validate(spark: SparkSession): Unit
}
