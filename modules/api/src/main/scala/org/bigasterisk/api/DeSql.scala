package org.bigasterisk.api

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * One step of a decomposed SQL query — a node of the query's plan, together with the
 * intermediate data it produces.
 *
 * Steps are what makes step-through debugging of a SQL query possible: instead of
 * seeing only the final answer, you inspect the rows flowing out of each constituent
 * part, the way a watchpoint exposes an intermediate value in a conventional debugger.
 *
 * @group desql
 */
trait QueryStep {

  /** Position in the decomposition. Leaves come first, the final result last. */
  def id: Int

  /** The relational operator at this step, e.g. `"Filter"`, `"Join"`, `"Aggregate"`. */
  def operator: String

  /**
   * A readable rendering of what the operator does — the filter condition, the join
   * keys, the grouping and aggregate expressions.
   */
  def detail: String

  /** Ids of the steps feeding this one, in operand order (left input first). */
  def childIds: Seq[Int]

  /** The schema of this step's intermediate result. */
  def schema: StructType

  /**
   * The whole sub-query this step computes, as a plan tree.
   *
   * [[detail]] is the step's *own* operator — a join condition, a grouping list. This is
   * everything beneath it as well, which is what "the sub-query at this point" actually
   * means: the scans it reads, the filters already applied, the joins already made.
   * Printing it answers "what have I got here?" without running anything.
   */
  def plan: String

  /**
   * The intermediate rows produced at this step.
   *
   * Materialising this runs the sub-query rooted here — the work up to this point in
   * the plan, and no more. It is a normal `DataFrame`, so it can be filtered, counted
   * or joined like any other.
   */
  def data: DataFrame

  /**
   * The conditional sub-operations of this step — the branches of its filters, `IF`s
   * and `CASE WHEN`s, each paired with the input rows that take it.
   *
   * Empty for steps with no conditional expressions, and for steps with more than one
   * input (a join's condition is already reflected in which rows survive the join).
   */
  def branches: Seq[Branch]
}

/**
 * One conditional sub-operation of a step: a branch its expressions can take, and the
 * input rows that take it.
 *
 * A step that passes every row through — a projection, say — tells you nothing about
 * which records a fault touched. Its *branches* do: of the records entering
 * `CASE WHEN amount > 1000 THEN -amount ELSE amount END`, only some take the first
 * arm. Branches are what make an operation's participation observable at the record
 * level, which is what fault localisation over operations needs.
 *
 * @group desql
 */
trait Branch {

  /** The condition, as SQL text. */
  def description: String

  /** The step's input rows that satisfy this branch's condition. */
  def data: DataFrame
}

/**
 * Step-through debugging for Spark SQL: decompose a query into its constituent parts
 * and inspect the intermediate data at each one.
 *
 * Obtain an instance from [[BigAsterisk.desql]].
 *
 * {{{
 * val df = spark.sql("SELECT c.name, SUM(o.amount) FROM orders o " +
 *                    "JOIN customers c ON o.cid = c.cid GROUP BY c.name")
 *
 * BigAsterisk.desql(spark).decompose(df).foreach { step =>
 *   println(s"[${step.id}] ${step.operator} ${step.detail}")
 *   step.data.show()
 * }
 * }}}
 *
 * @group desql
 */
trait DeSqlSupport {

  /**
   * Breaks `df` into the sequence of steps its plan describes, ordered so that every
   * step appears after the steps feeding it.
   *
   * Nodes that do not change the data — table aliases and similar bookkeeping — are
   * folded away, so each step corresponds to something a reader would recognise as a
   * part of their query.
   */
  def decompose(df: DataFrame): Seq[QueryStep]

  /**
   * Decomposes the query text `sql` in the context of `spark`.
   *
   * Equivalent to `decompose(spark.sql(sql))`, and provided so callers holding a query
   * string do not have to build the DataFrame first.
   */
  def decompose(spark: SparkSession, sql: String): Seq[QueryStep] =
    decompose(spark.sql(sql))
}
