package org.apache.spark.sql.fuzz

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.classic.{ExpressionColumnNode, Dataset => ClassicDataset, SparkSession => ClassicSparkSession}
import org.apache.spark.sql.desql.DeSqlEngine
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types.StructType

import org.bigasterisk.api._

/**
 * Fuzz testing for Spark SQL.
 *
 * ==One loop, three fuzzers==
 * BigFuzz, DepFuzz and NaturalFuzz differ in exactly one decision: where a generated
 * value comes from. Random values are the baseline; values spliced out of observed data
 * look real; values drawn from a pool shared between joined columns survive the join.
 * Everything else — the loop, the coverage measure, the corpus — is common, so it is
 * written once here and selected by [[MutationStrategy]].
 *
 * ==Coverage==
 * The query's branches, the same ones [[DeSqlEngine]] exposes for a step, are the
 * coverage targets: a `Filter` condition and its negation, each arm of a `CASE WHEN`.
 * An input that reaches a branch nothing had reached is kept and mutated further, which
 * is what makes the campaign a search rather than a sampler.
 *
 * Branches are evaluated in one aggregation per distinct input plan, not one query per
 * branch, so the cost per iteration stays close to the cost of running the query.
 */
class FuzzEngine extends FuzzSupport {

  override def fuzz(query: String, seeds: Map[String, DataFrame], config: FuzzConfig): FuzzResult = {
    require(seeds.nonEmpty, "at least one seed table is required")

    val spark = seeds.head._2.sparkSession match {
      case c: ClassicSparkSession => c
      case other =>
        throw new UnsupportedOperationException(
          "Fuzzing needs a classic SparkSession; this one is " +
          s"${other.getClass.getName}. Spark Connect sessions are not supported: " +
          "branch coverage is read from the driver-side analyzed plan, which a Connect " +
          "client does not hold.")
    }

    val schemas = seeds.map { case (name, df) => name -> df.schema }
    val pools = poolsFor(seeds, query, spark, config.strategy)
    val random = new Random(config.seed)

    // Branch targets are read from the query as analysed against the seed tables, so
    // the denominator does not move as generated data changes.
    val targets = branchTargets(spark, query)

    val covered = mutable.LinkedHashSet.empty[String]
    val failures = mutable.ArrayBuffer.empty[FuzzFailure]
    var empties = 0
    var iterationsRun = 0
    var abstracted = 0

    // Framework abstraction: analyse the query once against the seed views, then
    // interpret that plan with generated rows substituted at the leaves. No planning,
    // no scheduling and no task setup per iteration — and no re-analysis either.
    val abstraction: Option[(LogicalPlan, Map[String, String])] =
      if (!config.abstractFramework) None
      else {
        val analyzed = spark.sql(query).queryExecution.analyzed
        LocalDataflow.leafTables(analyzed, schemas).map(analyzed -> _)
      }

    // Profile once: which regions decide which branch, what each seed row does at each
    // branch, and which datasets the joins tie together. Both splicing strategies work
    // from this; the random strategy ignores it.
    val profile: Option[Profile] =
      if (config.strategy == MutationStrategy.Random) None
      else {
        val analyzed = spark.sql(query).queryExecution.analyzed
        LocalDataflow.leafTables(analyzed, schemas).map { leafToTable =>
          val influences = BranchProfiler.influences(analyzed, targets, leafToTable)
          val reduced = seeds.map { case (table, df) =>
            val rows = df.collect().toSeq
            val vectors = BranchProfiler.pathVectors(table, rows, df.schema, influences)
            // keep a sample per distinct path vector: rows that decide every branch the
            // same way are interchangeable, so the rest is search space for nothing
            table -> BranchProfiler.minimise(rows, vectors, config.rowsPerVector)
          }
          Profile(influences, reduced, BranchProfiler.joinConstraints(analyzed, leafToTable))
        }
      }

    // The corpus starts from the seed rows and grows with inputs that found new ground.
    val corpus = mutable.ArrayBuffer(seeds.map { case (n, df) => n -> df.collect().toSeq }.toMap)

    withRestoredViews(spark, seeds) {
      var i = 0
      while (i < config.iterations) {
        val candidate = generate(schemas, pools, corpus, config, random, profile, covered)
        iterationsRun += 1

        val outcome = abstraction.flatMap { case (plan, leafToTable) =>
          runAbstracted(plan, leafToTable, candidate, targets)
        }

        val (failed, empty, reached) = outcome match {
          case Some(result) => abstracted += 1; result
          case None =>
            registerViews(spark, candidate, schemas)
            runOnSpark(spark, query, targets)
        }

        failed.foreach { message =>
          failures += FuzzFailure(i, message, candidate)
          // an input that breaks the query is worth mutating further
          if (config.guided) corpus += candidate
        }
        if (empty) empties += 1
        val fresh = reached -- covered
        covered ++= reached
        if (config.guided && fresh.nonEmpty) corpus += candidate
        i += 1
      }
    }

    FuzzResult(
      iterationsRun, failures.toSeq, covered.toSet, targets.map(_._2.size).sum, empties,
      abstracted)
  }

  /** What one iteration produced: a failure message, whether it was empty, and coverage. */
  private type Iteration = (Option[String], Boolean, Set[String])

  /**
   * One iteration without Spark, or `None` if the interpreter cannot take it — in which
   * case the caller runs it on Spark rather than reporting a result it did not compute.
   */
  private def runAbstracted(
      plan: LogicalPlan,
      leafToTable: Map[String, String],
      candidate: Map[String, Seq[Row]],
      targets: Seq[(LogicalPlan, Seq[Expression])]): Option[Iteration] = {
    val byLeaf = leafToTable.map { case (leafKey, table) => leafKey -> candidate(table) }

    LocalDataflow.evaluate(plan, byLeaf) match {
      case LocalDataflow.Unsupported(_) => None
      case LocalDataflow.Failed(e) =>
        Some((Some(s"${e.getClass.getSimpleName}: ${e.getMessage}"), false, Set.empty))
      case LocalDataflow.Rows(rows) =>
        val reached = targets.foldLeft(Option(Set.empty[String])) {
          case (None, _) => None
          case (Some(acc), (input, conditions)) =>
            LocalDataflow.reached(input, conditions, byLeaf).map { indices =>
              acc ++ indices.map(i => describe(conditions(i)))
            }
        }
        // if coverage cannot be measured locally, fall back rather than under-report it
        reached.map(r => (None, rows.isEmpty, r))
    }
  }

  /** One iteration the ordinary way, against the registered views. */
  private def runOnSpark(
      spark: ClassicSparkSession,
      query: String,
      targets: Seq[(LogicalPlan, Seq[Expression])]): Iteration =
    try {
      val rows = spark.sql(query).collect()
      (None, rows.isEmpty, coverageOf(spark, query, targets))
    } catch {
      case NonFatal(e) => (Some(s"${e.getClass.getSimpleName}: ${e.getMessage}"), false, Set.empty)
    }

  /**
   * Restores the caller's views afterwards.
   *
   * The campaign works by swapping generated data in under the query's own table names,
   * so leaving the session pointing at the last candidate would be a nasty surprise.
   */
  private def withRestoredViews[A](spark: ClassicSparkSession, seeds: Map[String, DataFrame])(
      body: => A): A =
    try body
    finally seeds.foreach { case (name, df) => df.createOrReplaceTempView(name) }

  private def registerViews(
      spark: ClassicSparkSession,
      tables: Map[String, Seq[Row]],
      schemas: Map[String, StructType]): Unit =
    tables.foreach { case (name, rows) =>
      spark.createDataFrame(rows.asJava, schemas(name)).createOrReplaceTempView(name)
    }

  /**
   * What profiling learned about the query: which regions decide which branch, the
   * corpus reduced to a sample per distinct path vector, and the join equalities that
   * tie datasets together.
   */
  private case class Profile(
      influences: Seq[BranchProfiler.Influence],
      corpus: Map[String, Seq[(Row, BranchProfiler.PathVector)]],
      joins: Seq[Map[String, Set[String]]])

  /**
   * One candidate input, built by splicing rather than by drawing values independently.
   *
   * This is where the strategies part company:
   *
   *   - [[MutationStrategy.Random]] draws each value for its column's type.
   *   - [[MutationStrategy.Natural]] takes a row from the corpus and splices into it the
   *     columns that decide a branch nothing has reached yet, taken from a row that does
   *     reach it. The result is made only of parts that occurred in real data, and it is
   *     aimed at new coverage rather than at random.
   *   - [[MutationStrategy.CoDependent]] does the same, then repairs the join equalities
   *     across datasets so the spliced rows still match — mutating co-dependent regions
   *     jointly rather than one side at a time.
   */
  private def generate(
      schemas: Map[String, StructType],
      pools: Map[(String, String), ValuePool],
      corpus: mutable.ArrayBuffer[Map[String, Seq[Row]]],
      config: FuzzConfig,
      random: Random,
      profile: Option[Profile],
      covered: collection.Set[String]): Map[String, Seq[Row]] = {

    val basis = corpus(random.nextInt(corpus.length))

    val generated = schemas.map { case (table, schema) =>
      val rows = (0 until config.rowsPerTable).map { _ =>
        (config.strategy, profile) match {
          case (MutationStrategy.Random, _) | (_, None) =>
            drawRow(table, schema, pools, basis.get(table), config.strategy, random)
          case (_, Some(p)) =>
            spliceRow(table, schema, p, covered, pools, basis.get(table), config, random)
        }
      }
      table -> rows
    }

    (config.strategy, profile) match {
      case (MutationStrategy.CoDependent, Some(p)) => repairJoins(generated, schemas, p, random)
      case _                                       => generated
    }
  }

  /** A row drawn value by value, with no reference to what decides anything. */
  private def drawRow(
      table: String,
      schema: StructType,
      pools: Map[(String, String), ValuePool],
      basis: Option[Seq[Row]],
      strategy: MutationStrategy,
      random: Random): Row =
    Row.fromSeq(schema.fields.map { field =>
      value(table, field.name, field.dataType, pools, basis, schema, strategy, random)
    }.toIndexedSeq)

  /**
   * A row built by interleaving: a base row from the corpus, with the columns that decide
   * some branch spliced in from a row that reaches it.
   *
   * Preference goes to a branch nothing has covered yet, which is what turns splicing
   * from a way of making plausible rows into a way of making progress. When everything is
   * covered, any branch will do, so the corpus keeps moving.
   */
  private def spliceRow(
      table: String,
      schema: StructType,
      profile: Profile,
      covered: collection.Set[String],
      pools: Map[(String, String), ValuePool],
      basis: Option[Seq[Row]],
      config: FuzzConfig,
      random: Random): Row = {

    val rows = profile.corpus.getOrElse(table, Seq.empty)
    if (rows.isEmpty) {
      return drawRow(table, schema, pools, basis, config.strategy, random)
    }

    val base = rows(random.nextInt(rows.length))._1
    val decidedHere = profile.influences.zipWithIndex.filter {
      case (influence, _) => influence.columns.contains(table)
    }

    if (decidedHere.isEmpty) {
      // Nothing to aim at — the query has no branch this table decides. Splicing still
      // has to produce something new, so mix a random subset of columns from another row
      // and lean harder on the boundary set, which is the only remaining source of
      // interesting behaviour. Returning the base row unchanged here silently stopped a
      // campaign from ever finding an arithmetic overflow.
      val donor = rows(random.nextInt(rows.length))._1
      val columns = schema.fields.map(_.name).filter(_ => random.nextBoolean()).toSet
      val spliced = splice(base, donor, columns, schema)
      return if (random.nextInt(3) == 0) perturb(spliced, schema, random) else spliced
    }

    val uncovered = decidedHere.filterNot { case (influence, _) => covered.contains(influence.condition) }
    val (influence, index) =
      if (uncovered.nonEmpty) uncovered(random.nextInt(uncovered.length))
      else decidedHere(random.nextInt(decidedHere.length))

    // a donor that makes this branch true; if none does, splice from anywhere so the
    // candidate still differs from its base
    val satisfying = rows.filter(_._2.bits.lift(index).flatten.contains(true))
    val donor =
      if (satisfying.nonEmpty) satisfying(random.nextInt(satisfying.length))._1
      else rows(random.nextInt(rows.length))._1

    val spliced = splice(base, donor, influence.columns.getOrElse(table, Set.empty), schema)

    // a tenth of values still come from the boundary set, whatever the strategy: no
    // amount of real-looking data finds the crash on an empty string or an overflow
    if (random.nextInt(10) == 0) perturb(spliced, schema, random) else spliced
  }

  /** Takes `columns` from `donor` and everything else from `base`. */
  private def splice(base: Row, donor: Row, columns: Set[String], schema: StructType): Row =
    Row.fromSeq(schema.fields.zipWithIndex.map { case (field, i) =>
      if (columns.contains(field.name)) donor.get(i) else base.get(i)
    }.toIndexedSeq)

  /** Replaces one column with a boundary value. */
  private def perturb(row: Row, schema: StructType, random: Random): Row = {
    val i = random.nextInt(schema.fields.length)
    val boundaries = ValuePool.boundaryValues(schema.fields(i).dataType)
    if (boundaries.isEmpty) row
    else Row.fromSeq(schema.fields.indices.map { j =>
      if (j == i) boundaries(random.nextInt(boundaries.length)) else row.get(j)
    })
  }

  /**
   * Repairs the join equalities across the generated datasets.
   *
   * Splicing each table independently is not enough when a join ties them together: the
   * two sides drift apart, nothing matches, and the query returns nothing. For each
   * equality a value is chosen once and written into every table it links, so the
   * generated rows still join — the co-dependent regions mutated jointly rather than one
   * at a time.
   */
  private def repairJoins(
      generated: Map[String, Seq[Row]],
      schemas: Map[String, StructType],
      profile: Profile,
      random: Random): Map[String, Seq[Row]] = {
    if (profile.joins.isEmpty) return generated

    var out = generated
    profile.joins.foreach { constraint =>
      // draw the shared value from what the linked columns actually contain
      val candidates = constraint.toSeq.flatMap { case (table, columns) =>
        val schema = schemas(table)
        out.getOrElse(table, Seq.empty).flatMap { row =>
          columns.toSeq.flatMap(c => Option(row.get(schema.fieldIndex(c))))
        }
      }.distinct
      if (candidates.nonEmpty) {
        val shared = candidates(random.nextInt(candidates.length))
        out = out.map { case (table, rows) =>
          constraint.get(table) match {
            case None => table -> rows
            case Some(columns) =>
              val schema = schemas(table)
              // repair a prefix rather than every row, so the campaign still explores
              // rows that do not match
              val repaired = rows.zipWithIndex.map { case (row, i) =>
                if (i % 2 == 0) {
                  Row.fromSeq(schema.fields.zipWithIndex.map { case (field, j) =>
                    if (columns.contains(field.name)) shared else row.get(j)
                  }.toIndexedSeq)
                } else row
              }
              table -> repaired
          }
        }
      }
    }
    out
  }

  /** One generated value, for the strategies that draw rather than splice. */
  private def value(
      table: String,
      column: String,
      dataType: org.apache.spark.sql.types.DataType,
      pools: Map[(String, String), ValuePool],
      basis: Option[Seq[Row]],
      schema: StructType,
      strategy: MutationStrategy,
      random: Random): Any = {

    // A tenth of values come from the boundary set whatever the strategy: plausible data
    // alone will not find the crash on an empty string or an overflowing sum.
    val boundaries = ValuePool.boundaryValues(dataType)
    if (boundaries.nonEmpty && random.nextInt(10) == 0) {
      return boundaries(random.nextInt(boundaries.length))
    }

    strategy match {
      case MutationStrategy.Random =>
        ValuePool.randomValue(dataType, random)
      case _ =>
        pools.get((table, column)) match {
          case Some(pool) if !pool.isEmpty => pool.pick(random)
          case _ =>
            basis.flatMap { rows =>
              if (rows.isEmpty) None
              else {
                val row = rows(random.nextInt(rows.length))
                val i = schema.fieldIndex(column)
                if (i < row.length && !row.isNullAt(i)) Some(row.get(i)) else None
              }
            }.getOrElse(ValuePool.randomValue(dataType, random))
        }
    }
  }

  /**
   * The value pool for every column.
   *
   * For [[MutationStrategy.CoDependent]], columns linked by a join equality share one
   * pool, which is the whole point: mutate a join key freely and no row survives the
   * join, so the query returns nothing and the campaign learns nothing.
   */
  private def poolsFor(
      seeds: Map[String, DataFrame],
      query: String,
      spark: ClassicSparkSession,
      strategy: MutationStrategy): Map[(String, String), ValuePool] = {

    if (strategy == MutationStrategy.Random) return Map.empty

    val perColumn: Map[(String, String), ValuePool] = seeds.flatMap { case (table, df) =>
      ValuePool.of(df.collect().toSeq, df.schema).map { case (column, pool) =>
        (table, column) -> pool
      }
    }

    if (strategy != MutationStrategy.CoDependent) return perColumn

    val linked = FuzzEngine.joinedColumnNames(spark.sql(query).queryExecution.analyzed)
    if (linked.isEmpty) return perColumn

    // Union the pools of every column whose name participates in a join equality. Names
    // rather than attributes: a join between orders.cid and customers.cid should pool
    // both sides, and that is exactly what matching on the name does.
    val byName: Map[String, IndexedSeq[Any]] = perColumn.toSeq
      .groupBy { case ((_, column), _) => column }
      .map { case (column, entries) => column -> entries.flatMap(_._2.values).distinct.toIndexedSeq }

    perColumn.map { case (key @ (_, column), pool) =>
      val shared = linked
        .find { case (a, b) => a == column || b == column }
        .map { case (a, b) => (byName.getOrElse(a, IndexedSeq.empty) ++
          byName.getOrElse(b, IndexedSeq.empty)).distinct }
      key -> shared.map(ValuePool(_)).getOrElse(pool)
    }
  }

  /** Branch conditions of the query, grouped by the plan they are evaluated against. */
  private def branchTargets(
      spark: ClassicSparkSession,
      query: String): Seq[(LogicalPlan, Seq[Expression])] =
    DeSqlEngine
      .stepNodes(spark.sql(query).queryExecution.analyzed)
      .flatMap { node =>
        val conditions = DeSqlEngine.branchConditions(node.plan).flatMap(refine)
        if (conditions.isEmpty) None else Some(node.plan.children.head -> conditions.distinct)
      }

  /**
   * A condition and the conjuncts it is made of.
   *
   * `WHERE a AND b` is one condition to the query but two decisions to a fuzzer, and they
   * are usually decided by different columns. Profiling each conjunct separately is what
   * lets splicing combine a row that satisfies one with a row that satisfies the other —
   * a combination that mutating either alone would essentially never produce.
   */
  private def refine(condition: Expression): Seq[Expression] = {
    def conjuncts(e: Expression): Seq[Expression] = e match {
      case org.apache.spark.sql.catalyst.expressions.And(l, r) => conjuncts(l) ++ conjuncts(r)
      case other                                               => Seq(other)
    }
    val parts = conjuncts(condition)
    if (parts.size > 1) condition +: parts else parts
  }

  /**
   * Which branches the current data reaches.
   *
   * One aggregation per distinct input plan rather than one query per branch, so the
   * per-iteration cost stays close to the cost of running the query itself.
   */
  private def coverageOf(
      spark: ClassicSparkSession,
      query: String,
      targets: Seq[(LogicalPlan, Seq[Expression])]): Set[String] = {
    if (targets.isEmpty) return Set.empty

    // Re-analyse against the current data: the plans in `targets` were resolved against
    // the seed views and no longer refer to the registered relations.
    val current = branchTargets(spark, query)

    current.flatMap { case (input, conditions) =>
      try {
        val df = ClassicDataset.ofRows(spark, input)
        val indicators = conditions.zipWithIndex.map { case (c, i) =>
          max(new Column(ExpressionColumnNode(c))).as(s"__reached_$i")
        }
        val summary = df.agg(indicators.head, indicators.tail.toIndexedSeq: _*).collect().head
        conditions.zipWithIndex.collect {
          case (c, i) if !summary.isNullAt(i) && summary.getBoolean(i) => describe(c)
        }
      } catch {
        // a branch that cannot be evaluated against this data simply goes unreached
        case NonFatal(_) => Seq.empty
      }
    }.toSet
  }

  private def describe(e: Expression): String =
    try e.sql catch { case NonFatal(_) => e.toString }
}

object FuzzEngine {

  /**
   * Pairs of column names tied together by a join equality.
   *
   * These are the co-dependence constraints DepFuzz is named for: mutate one side
   * freely and the rows stop matching, so the query returns nothing.
   */
  def joinedColumnNames(plan: LogicalPlan): Seq[(String, String)] =
    plan.collect { case j: Join => j.condition }.flatten.flatMap { condition =>
      condition.collect {
        case EqualTo(l: AttributeReference, r: AttributeReference) => (l.name, r.name)
      }
    }.distinct
}
