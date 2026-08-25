package org.apache.spark.sql.bigdebug

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.watchpoint.WatchpointStrategy

/**
 * Installs BigDebug's planning hooks into a session.
 *
 * One extension per tool. BigDebug contributes two operators — the watchpoint and the
 * crash-culprit guard — and both arrive together under the tool's own name, so enabling
 * BigDebug never means naming some other tool's extension.
 *
 * Registered through [[org.bigasterisk.spark4.Spark4Binding.requiredConf]]. Spark takes
 * a comma-separated list, so each tool's hooks stay independent of the others'.
 */
class BigDebugExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(_ => WatchpointStrategy)
    extensions.injectPlannerStrategy(_ => CrashCulpritStrategy)
  }
}
