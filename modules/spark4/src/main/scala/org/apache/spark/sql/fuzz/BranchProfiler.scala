package org.apache.spark.sql.fuzz

import scala.util.control.NonFatal

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BasePredicate, BindReferences, Expression, Predicate}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Which parts of which datasets decide each branch, and how each row decides it.
 *
 * ==Why this exists==
 * Knowing *that* a query has branches is not enough to fuzz it well. What a fuzzer needs
 * is which rows and which columns of which dataset influence each branching decision —
 * so it can mutate the regions that matter, leave the rest alone, and tell whether a
 * candidate is likely to reach somewhere new.
 *
 * The techniques this serves obtain that by taint analysis: instrumenting each branch
 * predicate and tracking `(dataset, column, row)` tags through user code. Under a SQL
 * front end it is not an approximation at all. A predicate's referenced attributes carry
 * expression ids, and a leaf relation's output carries the same ids, so the mapping from
 * a branch to the columns that decide it is exact and reads straight off the plan.
 *
 * ==Path vectors==
 * Evaluating every branch against every row gives each row a bit per branch — its path
 * vector. Two rows with the same vector are interchangeable as far as the query's control
 * flow is concerned, which is what makes both minimisation and splicing possible:
 * a corpus can be reduced to a few rows per distinct vector, and a candidate can be built
 * by taking the deciding columns from a row that reaches a branch and the rest from
 * somewhere else.
 */
object BranchProfiler {

  /**
   * One branch, and the dataset regions that decide it.
   *
   * @param condition   the predicate, as SQL text — the identity used for coverage
   * @param columns     the columns that decide it, by table
   * @param bound       the predicate bound to one table's schema, when it depends on a
   *                    single table and can therefore be evaluated per row
   * @param table       that table, when there is exactly one
   */
  case class Influence(
      condition: String,
      columns: Map[String, Set[String]],
      bound: Option[Expression],
      table: Option[String]) {

    /** True when this branch is decided by more than one dataset. */
    def isJoint: Boolean = columns.size > 1

    /** Every table that has a say in this branch. */
    def tables: Set[String] = columns.keySet
  }

  /**
   * A row's answer to every branch, as a bit per branch.
   *
   * `None` where the branch is not decided by this row's table, so an unrelated branch
   * does not make two otherwise-interchangeable rows look different.
   */
  case class PathVector(bits: IndexedSeq[Option[Boolean]]) {
    /** A compact rendering, for grouping and for reporting: `1`, `0`, `-` for absent. */
    override def toString: String =
      bits.map { case Some(true) => '1'; case Some(false) => '0'; case None => '-' }.mkString

    /** Branch positions this row makes true. */
    def satisfied: Set[Int] = bits.zipWithIndex.collect { case (Some(true), i) => i }.toSet
  }

  /**
   * Works out which columns decide each branch.
   *
   * @param branches   each branch condition paired with the plan it is evaluated against
   * @param leafTables which table each leaf of the plan reads, by [[LocalDataflow.leafKey]]
   */
  def influences(
      plan: LogicalPlan,
      branches: Seq[(LogicalPlan, Seq[Expression])],
      leafTables: Map[String, String]): Seq[Influence] = {

    // exprId -> (table, column), read straight off the leaves
    val ownership: Map[Long, (String, String)] =
      plan.collect { case leaf: LeafNode => leaf }.flatMap { leaf =>
        leafTables.get(LocalDataflow.leafKey(leaf)).toSeq.flatMap { table =>
          leaf.output.map(a => a.exprId.id -> (table, a.name))
        }
      }.toMap

    val leafOutputs: Map[String, Seq[Attribute]] =
      plan.collect { case leaf: LeafNode => leaf }.flatMap { leaf =>
        leafTables.get(LocalDataflow.leafKey(leaf)).map(_ -> leaf.output)
      }.toMap

    branches.flatMap { case (_, conditions) => conditions }.distinct.map { condition =>
      val referenced = condition.references.toSeq.flatMap(a => ownership.get(a.exprId.id))
      val columns = referenced
        .groupBy(_._1)
        .map { case (table, entries) => table -> entries.map(_._2).toSet }

      // A branch decided by exactly one table, all of whose references reach that
      // table's leaf, can be evaluated row by row.
      val bound = columns.keys.toSeq match {
        case Seq(single) =>
          leafOutputs.get(single).flatMap { output =>
            val ids = output.map(_.exprId.id).toSet
            if (condition.references.forall(a => ids.contains(a.exprId.id)))
              try Some(BindReferences.bindReference(condition, output))
              catch { case NonFatal(_) => None }
            else None
          }
        case _ => None
      }

      Influence(describe(condition), columns, bound, columns.keys.toSeq match {
        case Seq(single) => Some(single)
        case _           => None
      })
    }
  }

  /**
   * The equality constraints a query's joins impose between datasets.
   *
   * Each entry is one equality, as the columns it ties together per table. A join is what
   * makes two datasets co-dependent: mutate one side of the equality freely and no row
   * survives, so the query returns nothing and a campaign learns nothing.
   *
   * Resolved through expression ids rather than by matching column names, so a join
   * between two columns that happen to share a name is not confused with one between
   * columns that genuinely correspond.
   */
  def joinConstraints(
      plan: LogicalPlan,
      leafTables: Map[String, String]): Seq[Map[String, Set[String]]] = {
    import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo}
    import org.apache.spark.sql.catalyst.plans.logical.Join

    val ownership: Map[Long, (String, String)] =
      plan.collect { case leaf: LeafNode => leaf }.flatMap { leaf =>
        leafTables.get(LocalDataflow.leafKey(leaf)).toSeq.flatMap { table =>
          leaf.output.map(a => a.exprId.id -> (table, a.name))
        }
      }.toMap

    plan.collect { case j: Join => j.condition }.flatten.flatMap { condition =>
      condition.collect {
        case EqualTo(l: AttributeReference, r: AttributeReference) =>
          Seq(l, r).flatMap(a => ownership.get(a.exprId.id))
            .groupBy(_._1)
            .map { case (table, entries) => table -> entries.map(_._2).toSet }
      }
    }.filter(_.size > 1).distinct
  }

  /**
   * The path vector of every row of `table`.
   *
   * One evaluation per row per branch, done locally — no Spark job, which is what makes
   * profiling affordable enough to redo as a corpus changes.
   */
  def pathVectors(
      table: String,
      rows: Seq[Row],
      schema: StructType,
      influences: Seq[Influence]): Seq[PathVector] = {
    if (rows.isEmpty) return Seq.empty

    val predicates: IndexedSeq[Option[BasePredicate]] = influences.map { influence =>
      if (influence.table.contains(table)) {
        influence.bound.flatMap { e =>
          try Some(Predicate.createInterpreted(e): BasePredicate)
          catch { case NonFatal(_) => None }
        }
      } else None
    }.toIndexedSeq

    val toCatalyst = CatalystTypeConverters.createToCatalystConverter(schema)
    rows.map { row =>
      val internal = toCatalyst(row).asInstanceOf[InternalRow]
      PathVector(predicates.map {
        case Some(p) =>
          try Some(p.eval(internal)) catch { case NonFatal(_) => None }
        case None    => None
      })
    }
  }

  /**
   * Reduces a corpus to a bounded sample per distinct path vector.
   *
   * Rows with the same vector are interchangeable as far as the query's control flow is
   * concerned, so keeping a handful of each preserves every behaviour the corpus can
   * reach while cutting what has to be searched. This is the data minimisation both
   * techniques rely on to make later iterations cheap.
   */
  def minimise(
      rows: Seq[Row],
      vectors: Seq[PathVector],
      perVector: Int): Seq[(Row, PathVector)] = {
    require(perVector > 0, s"perVector must be positive, got $perVector")
    rows.zip(vectors)
      .groupBy(_._2.toString)
      .values
      .flatMap(_.take(perVector))
      .toSeq
  }

  private def describe(e: Expression): String =
    try e.sql catch { case NonFatal(_) => e.toString }

  /** The schema a table's rows carry, for converting them to Catalyst form. */
  def schemaOf(output: Seq[Attribute]): StructType =
    StructType(output.map(a => StructField(a.name, a.dataType, a.nullable)))
}
