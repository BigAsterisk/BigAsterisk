package org.apache.spark.sql.ui

import scala.util.control.NonFatal
import scala.xml.{Node, NodeSeq, Unparsed}

import jakarta.servlet.http.HttpServletRequest

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.ui.{SparkUI, SparkUITab, UIUtils, WebUIPage}

import org.bigasterisk.api.{BigAsterisk, UdfRegistry}

/**
 * A debugging surface inside the Spark UI.
 *
 * ==Why this exists==
 * The technique this belongs to was an interactive environment, not a library: you set a
 * watchpoint, watched records stream past it, and looked at the record that killed a
 * task — while the job ran, from a browser. Reduced to an API, the same capability
 * becomes something you have to write code around, and the debugging loop that made it
 * useful disappears.
 *
 * So the tools' live state is published as a tab beside Spark's own. Nothing here
 * computes anything: every panel reads a registry the tools already keep, which is why
 * the tab costs nothing when no one is looking at it.
 *
 * ==What it costs the user==
 * Nothing. It attaches through `SparkPlugin`, so
 * `BigAsterisk.configure(SparkSession.builder())` — the one line that installs the
 * bindings — installs this too. There is no second call to remember and no code in the
 * job itself.
 */
private[spark] class BigAsteriskTab(ui: SparkUI, session: () => Option[SparkSession])
    extends SparkUITab(ui, "bigasterisk") {

  override val name: String = "BigAsterisk"

  attachPage(new OverviewPage(this, session))
  attachPage(new WatchpointsPage(this, session))
  attachPage(new BreakpointsPage(this, session))
  attachPage(new CrashesPage(this, session))
  attachPage(new LatencyPage(this, session))
  attachPage(new FunctionsPage(this, session))
}

/**
 * Shared rendering.
 *
 * A debugging panel is worth less than nothing if it lies, so every page here degrades
 * the same way: no session, no state, or a tool that throws all produce a sentence
 * saying so rather than an empty table that reads like "nothing is wrong".
 */
private[ui] abstract class BigAsteriskPage(
    parent: BigAsteriskTab,
    prefix: String,
    session: () => Option[SparkSession]) extends WebUIPage(prefix) {

  /** The page's title in the tab's sub-navigation. */
  def title: String

  /** The body, given a live session. */
  def body(spark: SparkSession, request: HttpServletRequest): Seq[Node]

  final override def render(request: HttpServletRequest): Seq[Node] = {
    val content = session() match {
      case None =>
        note("No active SparkSession. The tab appears as soon as a session exists.")
      case Some(spark) =>
        try body(spark, request)
        catch {
          case NonFatal(e) =>
            note(s"${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("")}")
        }
    }
    UIUtils.headerSparkPage(request, s"BigAsterisk — $title", navigation ++ content, parent)
  }

  /** Links to the other panels, so the tab is navigable without going back. */
  private def navigation: Seq[Node] = {
    val here = prefix
    val links = BigAsteriskPage.panels.map { case (path, label) =>
      val href = if (path.isEmpty) parent.basePath + "/bigasterisk/"
                 else parent.basePath + s"/bigasterisk/$path/"
      if (path == here) <li class="active"><a href={href}>{label}</a></li>
      else <li><a href={href}>{label}</a></li>
    }
    <ul class="nav nav-pills" style="margin-bottom: 14px;">{links}</ul>
  }

  protected def note(text: String): Node =
    <div class="alert alert-info" style="margin-top: 10px;">{text}</div>

  /** A table, or a sentence saying why there is none. */
  protected def table(headers: Seq[String], rows: Seq[Seq[Node]], empty: String): NodeSeq =
    if (rows.isEmpty) note(empty)
    else
      <table class="table table-bordered table-condensed table-striped sortable">
        <thead><tr>{headers.map(h => <th>{h}</th>)}</tr></thead>
        <tbody>{rows.map(cells => <tr>{cells.map(c => <td>{c}</td>)}</tr>)}</tbody>
      </table>

  /** Rows of data, rendered compactly. */
  protected def records(rows: Seq[org.apache.spark.sql.Row], limit: Int = 20): Node =
    if (rows.isEmpty) <span class="text-muted">none captured</span>
    else <pre style="margin: 0; max-height: 260px; overflow: auto;">{
      rows.take(limit).map(_.toString).mkString("\n") +
        (if (rows.size > limit) s"\n… ${rows.size - limit} more" else "")
    }</pre>

  protected def code(text: String): Node = <code>{text}</code>
}

private[ui] object BigAsteriskPage {
  /** The panels, in the order the tab lists them. */
  val panels: Seq[(String, String)] = Seq(
    "" -> "Overview",
    "watchpoints" -> "Watchpoints",
    "breakpoints" -> "Breakpoints",
    "crashes" -> "Crashes",
    "latency" -> "Latency",
    "functions" -> "Functions")
}

/** What is live right now, and what each panel would show. */
private[ui] class OverviewPage(parent: BigAsteriskTab, session: () => Option[SparkSession])
    extends BigAsteriskPage(parent, "", session) {

  val title = "Overview"

  def body(spark: SparkSession, request: HttpServletRequest): Seq[Node] = {
    def count(f: => Int): String = try f.toString catch { case NonFatal(_) => "—" }

    val rows = Seq(
      Seq(<a href={parent.basePath + "/bigasterisk/watchpoints/"}>Watchpoints</a>,
        <span>{count(BigAsterisk.watchpoints(spark).active.size)}</span>,
        <span>records matching a condition, captured as they flow past</span>),
      Seq(<a href={parent.basePath + "/bigasterisk/breakpoints/"}>Breakpoints</a>,
        <span>{count(BigAsterisk.breakpoints(spark).active.size)}</span>,
        <span>the state at a point in a query, regenerated on demand</span>),
      Seq(<a href={parent.basePath + "/bigasterisk/crashes/"}>Crash guards</a>,
        <span>{count(BigAsterisk.crashCulprit(spark).active.size)}</span>,
        <span>the record that killed a task, by partition and index</span>),
      Seq(<a href={parent.basePath + "/bigasterisk/latency/"}>Latency profiles</a>,
        <span>{count(BigAsterisk.perfdebug(spark).active.size)}</span>,
        <span>per-record cost, and which records carry the skew</span>),
      Seq(<a href={parent.basePath + "/bigasterisk/functions/"}>Functions</a>,
        <span>{count(UdfRegistry.size)}</span>,
        <span>what static analysis read inside your UDFs</span>))

    <div>
      <p>
        Live state from the tools attached to this application. Nothing here runs a job:
        each panel reads what the tools already recorded.
      </p>
      {table(Seq("Panel", "Active", "What it shows"), rows, "")}
      <h4>Attaching a tool</h4>
      <p>
        One line installs everything, including this tab:
      </p>
      <pre>spark = bigasterisk.configure(SparkSession.builder).getOrCreate()</pre>
      <p>
        After that a watchpoint is one more:
      </p>
      <pre>wp = bigasterisk.watchpoints(spark).watch(df, col("amount") &gt; 1000)
wp.df.groupBy("cid").sum("amount").collect()   # use wp.df in place of df</pre>
      <p>
        and its hits appear in the Watchpoints panel while the job runs.
      </p>
    </div>
  }
}

/** Records captured as they flowed past a condition. */
private[ui] class WatchpointsPage(parent: BigAsteriskTab, session: () => Option[SparkSession])
    extends BigAsteriskPage(parent, "watchpoints", session) {

  val title = "Watchpoints"

  def body(spark: SparkSession, request: HttpServletRequest): Seq[Node] = {
    val support = BigAsterisk.watchpoints(spark)
    Option(request.getParameter("reset")).foreach { id =>
      support.active.find(_.id == id).foreach(_.reset())
    }

    val rows = support.active.map { wp =>
      Seq(
        code(wp.id),
        code(wp.condition),
        <span>{wp.hits.toString}{if (wp.truncated) " (capture full)" else ""}</span>,
        records(wp.captured.toSeq),
        <a href={parent.basePath + s"/bigasterisk/watchpoints/?reset=${wp.id}"}>reset</a>)
    }

    table(Seq("Id", "Condition", "Hits", "Captured", ""), rows,
      "No watchpoints. Create one with " +
        "bigasterisk.watchpoints(spark).watch(df, col(\"amount\") > 1000), then use its " +
        "`.df` in place of `df`; matching records appear here as the job runs.")
  }
}

/** The state at a point in a query. */
private[ui] class BreakpointsPage(parent: BigAsteriskTab, session: () => Option[SparkSession])
    extends BigAsteriskPage(parent, "breakpoints", session) {

  val title = "Breakpoints"

  def body(spark: SparkSession, request: HttpServletRequest): Seq[Node] = {
    val support = BigAsterisk.breakpoints(spark)
    val active = support.active

    Option(request.getParameter("materialize")).foreach { id =>
      active.find(_.id == id).foreach(_.materialize())
    }
    Option(request.getParameter("release")).foreach { id =>
      active.find(_.id == id).foreach(_.release())
    }

    val show = Option(request.getParameter("show"))

    val rows = active.map { bp =>
      val action =
        if (bp.isMaterialized)
          <a href={parent.basePath + s"/bigasterisk/breakpoints/?release=${bp.id}"}>release</a>
        else
          <a href={parent.basePath + s"/bigasterisk/breakpoints/?materialize=${bp.id}"}>materialize</a>

      Seq(
        code(bp.id),
        code(bp.schema.fieldNames.mkString(", ")),
        <span>{if (bp.isMaterialized) "pinned" else "regenerated on demand"}</span>,
        <span>
          <a href={parent.basePath + s"/bigasterisk/breakpoints/?show=${bp.id}"}>inspect</a>
          {Unparsed("&nbsp;·&nbsp;")}{action}
        </span>)
    }

    val inspected = show.flatMap(id => active.find(_.id == id)).map { bp =>
      <div>
        <h4>{bp.id}</h4>
        <p>
          The rows flowing past this point. Materialising the query runs it
          <em>up to here and no further</em>.
        </p>
        {records(bp.state(20).toSeq)}
      </div>
    }.getOrElse(NodeSeq.Empty)

    table(Seq("Id", "Schema", "State", ""), rows,
      "No breakpoints. Create one with bigasterisk.breakpoints(spark).breakpoint(df); " +
        "setting it costs nothing until you inspect it.") ++ inspected
  }
}

/** The record that killed a task. */
private[ui] class CrashesPage(parent: BigAsteriskTab, session: () => Option[SparkSession])
    extends BigAsteriskPage(parent, "crashes", session) {

  val title = "Crashes"

  def body(spark: SparkSession, request: HttpServletRequest): Seq[Node] = {
    val rows = BigAsterisk.crashCulprit(spark).active.map { guard =>
      guard.culprit match {
        case Some(c) =>
          Seq(code(guard.id),
            <span class="text-danger">crashed</span>,
            <span>partition {c.partitionId.toString}, record {c.recordIndex.toString}</span>,
            <pre style="margin:0">{c.row.toString}</pre>,
            <pre style="margin:0; max-height:120px; overflow:auto">{c.error}</pre>)
        case None =>
          Seq(code(guard.id), <span>no failure</span>, <span>—</span>, <span>—</span>,
            <span>—</span>)
      }
    }

    table(Seq("Guard", "State", "Where", "Record", "Error"), rows,
      "No crash guards. Wrap a DataFrame with bigasterisk.crash_culprit(spark).guard(df) " +
        "and, if a task dies, the record that killed it appears here — partition and " +
        "index — instead of a stack trace.")
  }
}

/** Per-record cost. */
private[ui] class LatencyPage(parent: BigAsteriskTab, session: () => Option[SparkSession])
    extends BigAsteriskPage(parent, "latency", session) {

  val title = "Latency"

  def body(spark: SparkSession, request: HttpServletRequest): Seq[Node] = {
    val rows = BigAsterisk.perfdebug(spark).active.map { profile =>
      Seq(
        <span>{profile.records.toString}</span>,
        <span>{f"${profile.meanNanos / 1e6}%.4f ms"}</span>,
        <span>{f"${profile.skew}%.1fx"}</span>,
        <pre style="margin:0; max-height:200px; overflow:auto">{
          profile.slowest.take(10).map(_.toString).mkString("\n")
        }</pre>)
    }

    table(Seq("Records", "Mean", "Skew", "Slowest"), rows,
      "No latency profiles. Wrap a DataFrame with bigasterisk.perfdebug(spark).profile(df) " +
        "and the cost of each record is attributed back to it.") ++
      <p class="text-muted">
        On a small input the slowest record is usually the first one through a stage —
        it pays for code generation and task setup. Skew is about the data only once
        the per-record work is larger than that.
      </p>
  }
}

/** What was read inside the user's functions. */
private[ui] class FunctionsPage(parent: BigAsteriskTab, session: () => Option[SparkSession])
    extends BigAsteriskPage(parent, "functions", session) {

  val title = "Functions"

  def body(spark: SparkSession, request: HttpServletRequest): Seq[Node] = {
    val rows = UdfRegistry.names.toSeq.sorted.flatMap(UdfRegistry.lookup).map { profile =>
      Seq(
        code(s"${profile.name}(${profile.parameters.mkString(", ")})"),
        <span>{
          if (profile.isSolvable) "solvable"
          else if (profile.isComplete) "complete"
          else s"partial (${profile.unsupported.size} unread)"
        }</span>,
        <pre style="margin:0">{profile.branches.map(_.condition).mkString("\n")}</pre>,
        <span>{
          if (profile.influencing.isEmpty) "none"
          else profile.influencing.toSeq.sorted.mkString(", ")
        }</span>,
        <pre style="margin:0; max-height:120px; overflow:auto">{
          if (profile.unsupported.isEmpty) "—" else profile.unsupported.mkString("\n")
        }</pre>)
    }

    table(Seq("Function", "Read", "Branches", "Arguments that matter", "Not understood"),
      rows,
      "No functions read yet. A Scala UDF is read from its bytecode the first time a " +
        "tool meets it; a Python UDF is registered with bigasterisk.udf.register(spark, fn).") ++
      <p class="text-muted">
        A branch here is a condition inside your function, expressed over the columns the
        call site passes it. Those branches are what operation-level fault localisation
        scores and what test generation solves.
      </p>
  }
}

/**
 * The one entry point outside this package: everything else here is Spark-internal.
 * Public so the plugin, which lives with the rest of the binding, can attach the tab.
 */
object BigAsteriskTab {

  /**
   * Attaches the tab to a context's UI, once.
   *
   * Idempotent: a session created twice in one JVM — which notebooks do constantly —
   * must not stack duplicate tabs.
   */
  def attach(sc: SparkContext, session: () => Option[SparkSession]): Boolean =
    sc.ui match {
      case Some(ui) if !ui.getTabs.exists(_.name == "BigAsterisk") =>
        try {
          ui.attachTab(new BigAsteriskTab(ui, session))
          true
        } catch {
          case NonFatal(_) => false
        }
      case _ => false
    }
}
