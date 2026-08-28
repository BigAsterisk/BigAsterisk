package org.bigasterisk.spark4

import java.util.{Map => JMap}

import scala.util.control.NonFatal

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.ui.BigAsteriskTab

/**
 * Attaches the debugging tab to the Spark UI.
 *
 * ==Why a plugin==
 * The tab has to be attached to a live `SparkContext`, which does not exist when the
 * session is being configured. `SparkPlugin` is Spark's own answer to that: the driver
 * plugin is initialised with the context as soon as there is one.
 *
 * The alternative — asking users to call something after creating their session — is
 * exactly the kind of ceremony this project exists to remove. Installing the bindings
 * installs this, and a job that never opens the UI pays nothing for it: the tab renders
 * only when a request arrives, from registries the tools already keep.
 */
class BigAsteriskPlugin extends SparkPlugin {

  override def driverPlugin(): DriverPlugin = new DriverPlugin {

    override def init(sc: SparkContext, context: PluginContext): JMap[String, String] = {
      try {
        // The session is looked up per request rather than captured: a notebook may
        // create, stop and recreate one many times over a single context's life, and the
        // tab should follow whatever is current rather than hold a dead reference.
        BigAsteriskTab.attach(sc, () => SparkSession.getActiveSession
          .orElse(SparkSession.getDefaultSession))
      } catch {
        // A UI that cannot attach must never stop a job from running.
        case NonFatal(_) =>
      }
      java.util.Collections.emptyMap()
    }
  }

  /** Nothing runs on the executors: the tab is a driver-side view of driver-side state. */
  override def executorPlugin(): ExecutorPlugin = null
}
