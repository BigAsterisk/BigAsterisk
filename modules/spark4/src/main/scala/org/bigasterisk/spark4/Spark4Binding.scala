package org.bigasterisk.spark4

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.lineage.{TitianSQL, TraceCursor => TitianCursor}

import org.apache.spark.sql.desql.DeSqlEngine
import org.apache.spark.sql.perfdebug.PerfDebugEngine
import org.apache.spark.sql.vega.VegaEngine
import org.apache.spark.sql.watchpoint.Spark4Watchpoints

import org.bigasterisk.api.{DeSqlSupport, LineageSupport, PerfDebugSupport, SparkBinding, TraceCursor, VegaSupport, WatchpointSupport}

/**
 * The BigAsterisk binding for Apache Spark 4.x.
 *
 * Capture attaches through sanctioned Spark 4 extension points — a
 * `SparkSessionExtensions` columnar rule that inserts tap operators into the physical
 * plan, RDD subclassing, and task-completion listeners — plus a thin, documented
 * internals layer. Jobs run on a stock Spark distribution.
 *
 * Discovered at runtime by [[org.bigasterisk.api.BigAsterisk]] through
 * `META-INF/services/org.bigasterisk.api.SparkBinding`.
 */
class Spark4Binding extends SparkBinding {

  override def name: String = "spark4"

  /** Spark 4.x. The tap operators depend on the Spark 4 `SparkPlan`/codegen contract. */
  override def sparkVersions: String = "[4.0.0,5.0.0)"

  override def requiredConf: Map[String, String] = Map(
    "spark.sql.extensions" -> Spark4Binding.extensionClassNames.mkString(",")
  )

  override def validate(spark: SparkSession): Unit =
    Spark4Binding.checkExtensions(spark.conf.getOption("spark.sql.extensions"))

  override val lineage: LineageSupport = new Spark4Lineage

  override val desql: DeSqlSupport = new DeSqlEngine

  override val watchpoints: WatchpointSupport = new Spark4Watchpoints

  override val vega: VegaSupport = new VegaEngine()

  override val perfdebug: PerfDebugSupport = new PerfDebugEngine
}

object Spark4Binding {

  /**
   * The `SparkSessionExtensions` this binding installs.
   *
   * Spark accepts a comma-separated list, so each tool's planning hooks stay
   * independent of the others'.
   */
  val extensionClassNames: Seq[String] = Seq(
    classOf[org.apache.spark.sql.lineage.TitianSQLExtension].getName,
    classOf[org.apache.spark.sql.watchpoint.WatchpointExtension].getName,
    classOf[org.apache.spark.sql.perfdebug.PerfDebugExtension].getName)

  /** The lineage extension, kept as a named constant for diagnostics and docs. */
  val extensionClassName: String = extensionClassNames.head

  /**
   * Checks that `configured` — the session's `spark.sql.extensions` — installs this
   * binding's extension.
   *
   * Kept separate from [[Spark4Binding.validate]] so the failure message can be tested
   * without standing up a SparkSession whose conf is missing the extension (in local
   * mode every session in the JVM shares one SparkContext and inherits its conf).
   *
   * @throws IllegalStateException with guidance on how to build the session correctly.
   */
  def checkExtensions(configured: Option[String]): Unit = {
    val extensions = configured.getOrElse("")
    val present = extensions.split(',').map(_.trim).toSet
    val missing = extensionClassNames.filterNot(present.contains)
    if (missing.nonEmpty) {
      throw new IllegalStateException(
        s"""This SparkSession was created without ${if (missing.length == 1) "a BigAsterisk"
             else "the BigAsterisk"} SQL extension, so ${if (missing.length == 1) "that tool"
             else "those tools"} cannot attach. Spark reads spark.sql.extensions when the
           |session is built, which is why it cannot be set afterwards.
           |
           |Missing: ${missing.mkString(", ")}
           |
           |Build the session through BigAsterisk.configure:
           |    val spark = BigAsterisk.configure(SparkSession.builder()).getOrCreate()
           |
           |or set it yourself before creating the session:
           |    .config("spark.sql.extensions", "${extensionClassNames.mkString(",")}")
           |
           |Currently set: ${if (extensions.isEmpty) "<unset>" else extensions}""".stripMargin)
    }
  }
}

/** [[LineageSupport]] backed by the Spark 4 codegen tap engine. */
private[spark4] class Spark4Lineage extends LineageSupport {

  override def enableCapture(spark: SparkSession): Unit = TitianSQL.enableCapture(spark)

  override def disableCapture(spark: SparkSession): Unit = TitianSQL.disableCapture(spark)

  override def collectWithLineage(df: DataFrame): Array[(Row, Long)] =
    TitianSQL.collectWithLineage(df)

  override def trace(df: DataFrame, outputIds: Seq[Long]): TraceCursor =
    new Spark4TraceCursor(TitianSQL.trace(df, outputIds))

  override def backward(df: DataFrame, outputIds: Seq[Long]): Array[Long] =
    TitianSQL.backward(df, outputIds)

  override def showInputs(df: DataFrame, inputIds: Seq[Long]): Array[Row] =
    TitianSQL.showInputs(df, inputIds)

  override def releaseLineage(df: DataFrame): Unit = TitianSQL.releaseLineage(df)

  override def lineageSize(df: DataFrame): (Long, Long) = TitianSQL.lineageSize(df)

  override def resultIds(df: DataFrame): Array[Long] = TitianSQL.resultIds(df)

  override def traceJava(
      df: DataFrame,
      outputIds: java.util.List[java.lang.Number]): TraceCursor =
    new Spark4TraceCursor(TitianSQL.traceJava(df, outputIds))
}

/** Adapts the engine's cursor to the version-independent [[TraceCursor]]. */
private[spark4] class Spark4TraceCursor(private val underlying: TitianCursor)
  extends TraceCursor {

  override def ids: Array[Long] = underlying.ids

  override def atScan: Boolean = underlying.atScan

  override def goBack(branch: Int = 0): TraceCursor =
    new Spark4TraceCursor(underlying.goBack(branch))

  override def goNext(): TraceCursor = new Spark4TraceCursor(underlying.goNext())

  override def show(full: Boolean = false): Array[Row] = underlying.show(full)

  override def showJson(full: Boolean = false): Array[String] = underlying.showJson(full)
}
