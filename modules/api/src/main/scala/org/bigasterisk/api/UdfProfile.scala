package org.bigasterisk.api

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

/**
 * One conditional branch inside a user-defined function.
 *
 * @param condition the branch's condition, as SQL text over the function's *parameter
 *                  names*. Binding it to a query means substituting each parameter for
 *                  the argument expression the call site passes.
 * @param parameters the parameters the condition reads
 *
 * @group udf
 */
case class UdfBranch(condition: String, parameters: Set[String])

/**
 * One path through a user-defined function: what has to hold for control to reach a
 * particular `return`, and what that `return` produces.
 *
 * @param constraint the conjunction of branch outcomes leading here, as SQL text over
 *                   the parameter names. `"true"` for a function with no branches.
 * @param returns    the returned value as SQL text, when it is a literal. A path that
 *                   returns a computed value has `None` here: it is still a coverage
 *                   target, but a caller cannot invert it.
 * @param exact      whether the analysis understood every statement on this path.
 *                   An inexact path may be reached under conditions this constraint
 *                   does not describe, so it must not be used to *solve* for an input.
 *
 * @group udf
 */
case class UdfPath(constraint: String, returns: Option[String], exact: Boolean) {

  /** Whether this path can be used to solve for an input producing `value`. */
  def yields(value: String): Boolean = exact && returns.contains(value)
}

/**
 * What static analysis learned about the inside of a user-defined function.
 *
 * ==Why this exists==
 * A UDF is opaque to plan analysis. Every technique here that reasons about branches —
 * which operation is at fault, which input reaches which path, which column actually
 * influenced a result — stops at the boundary of one, and reports the whole call as a
 * single black-box operation over all of its arguments.
 *
 * The published tools cross that boundary by analysing the function's code:
 * source-to-source transformation for taint, symbolic execution of bytecode for path
 * constraints. A profile is the result of doing that, in a form the engines here can
 * use: branches, paths, and which parameters actually influence the output.
 *
 * ==Where profiles come from==
 * Python UDFs only, for now. The Python front end parses the function's own source and
 * registers what it found ([[UdfRegistry]]); nothing is analysed on the JVM side,
 * because the function is Python and its code is not on this side of the boundary. A
 * Scala or Java UDF arrives as a closure whose logic is JVM bytecode, which would need
 * a different analysis entirely — those are still black boxes, and every consumer says
 * so rather than guessing.
 *
 * @param name        the function's name, as it appears in the query plan
 * @param parameters  its parameters, in the order the call site passes arguments
 * @param branches    every conditional branch inside it
 * @param paths       every path through it
 * @param influencing the parameters that influence the returned value, by data flow or
 *                    by deciding which branch returns. A parameter absent from this set
 *                    provably cannot change the result.
 * @param unsupported anything the analysis could not read, as human-readable reasons.
 *                    A profile with entries here is partial, and says so, rather than
 *                    quietly under-reporting.
 *
 * @group udf
 */
case class UdfProfile(
    name: String,
    parameters: Seq[String],
    branches: Seq[UdfBranch] = Seq.empty,
    paths: Seq[UdfPath] = Seq.empty,
    influencing: Set[String] = Set.empty,
    unsupported: Seq[String] = Seq.empty) {

  require(name.nonEmpty, "a profile needs the function's name")

  /** True when the analysis read the whole function. */
  def isComplete: Boolean = unsupported.isEmpty

  /** True when every path is exact, so the function can be solved through. */
  def isSolvable: Boolean = isComplete && paths.nonEmpty && paths.forall(_.exact)

  /** The paths that produce `value`, usable for solving. */
  def pathsYielding(value: String): Seq[UdfPath] = paths.filter(_.yields(value))

  /** Whether the parameter at `index` can influence what the function returns. */
  def influences(index: Int): Boolean =
    parameters.lift(index).exists(influencing.contains)

  override def toString: String = {
    val state =
      if (isSolvable) "solvable"
      else if (isComplete) "complete"
      else s"partial (${unsupported.size} unread)"
    s"UdfProfile($name(${parameters.mkString(", ")}), ${branches.size} branches, " +
      s"${paths.size} paths, $state)"
  }
}

object UdfProfile {

  /**
   * Reads a profile from the line format the Python analyser emits.
   *
   * The format is tab-separated lines rather than JSON so that neither side needs a
   * parser or a dependency for it:
   *
   * {{{
   * udf         <name>      <param,param,...>
   * branch      <condition>                       <params>
   * path        <constraint>       <returns>      <exact|approximate>
   * influence   <param>
   * unsupported <reason>
   * }}}
   *
   * Conditions and constraints are SQL text over the parameter names, which is what
   * makes them usable: the JVM side parses them with Spark's own parser and substitutes
   * the call site's argument expressions, so no expression grammar is reimplemented
   * here.
   */
  def parse(lines: Seq[String]): UdfProfile = {
    var name: String = ""
    var parameters: Seq[String] = Seq.empty
    val branches = Seq.newBuilder[UdfBranch]
    val paths = Seq.newBuilder[UdfPath]
    val influencing = Set.newBuilder[String]
    val unsupported = Seq.newBuilder[String]

    lines.filter(_.trim.nonEmpty).foreach { line =>
      val fields = line.split('\t')
      fields.head match {
        case "udf" =>
          require(fields.length >= 2, s"malformed udf line: $line")
          name = fields(1)
          parameters = fields.lift(2).map(splitList).getOrElse(Seq.empty)
        case "branch" =>
          require(fields.length >= 3, s"malformed branch line: $line")
          branches += UdfBranch(fields(1), splitList(fields(2)).toSet)
        case "path" =>
          require(fields.length >= 4, s"malformed path line: $line")
          val returns = if (fields(2).isEmpty) None else Some(fields(2))
          paths += UdfPath(fields(1), returns, fields(3) == "exact")
        case "influence" =>
          require(fields.length >= 2, s"malformed influence line: $line")
          influencing += fields(1)
        case "unsupported" =>
          unsupported += fields.lift(1).getOrElse("unspecified")
        case other =>
          throw new IllegalArgumentException(s"unknown profile record '$other' in: $line")
      }
    }

    require(name.nonEmpty, "the profile has no udf line")
    UdfProfile(name, parameters, branches.result(), paths.result(),
      influencing.result(), unsupported.result())
  }

  private def splitList(field: String): Seq[String] =
    if (field.isEmpty) Seq.empty else field.split(',').toSeq.map(_.trim).filter(_.nonEmpty)
}

/**
 * The profiles known for this driver, by function name.
 *
 * ==Why a registry, and why keyed by name==
 * The analysis happens where the code is — in Python — while the techniques that need
 * it run on the JVM. A plan carries a Python UDF's *name*, so a name is what the two
 * sides can agree on.
 *
 * The consequence is worth stating plainly: two different functions registered under
 * one name are indistinguishable here, and the later registration wins. Register under
 * the name the query uses.
 *
 * Nothing is registered by default. An engine that finds no profile for a function
 * treats it as a black box exactly as it did before, so this can only add information,
 * never change an existing result.
 *
 * @group udf
 */
object UdfRegistry {

  private val profiles = new ConcurrentHashMap[String, UdfProfile]()

  /** Records `profile`, replacing any profile under the same name. */
  def register(profile: UdfProfile): Unit = profiles.put(profile.name, profile)

  /**
   * Records a profile in the line format of [[UdfProfile.parse]].
   *
   * This is the entry point the Python front end calls: an array of strings is the
   * richest structure Py4J marshals without ceremony.
   */
  def registerLines(lines: Array[String]): String = {
    val profile = UdfProfile.parse(lines.toSeq)
    register(profile)
    profile.toString
  }

  /** The profile for `name`, if one was registered. */
  def lookup(name: String): Option[UdfProfile] = Option(profiles.get(name))

  /** The names with a registered profile. */
  def names: Set[String] = profiles.keySet().asScala.toSet

  /** [[names]] for callers that cannot read a Scala collection — Java, and Py4J. */
  def registeredNames: Array[String] = names.toArray.sorted

  /** Forgets `name`. */
  def remove(name: String): Unit = profiles.remove(name)

  /** Forgets every profile. */
  def clear(): Unit = profiles.clear()

  /** How many profiles are registered. */
  def size: Int = profiles.size()
}
