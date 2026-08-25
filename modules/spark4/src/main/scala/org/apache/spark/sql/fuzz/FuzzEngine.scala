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

    // The corpus starts from the seed rows and grows with inputs that found new ground.
    val corpus = mutable.ArrayBuffer(seeds.map { case (n, df) => n -> df.collect().toSeq }.toMap)

    withRestoredViews(spark, seeds) {
      var i = 0
      while (i < config.iterations) {
        val candidate = generate(schemas, pools, corpus, config, random)
        registerViews(spark, candidate, schemas)
        iterationsRun += 1

        try {
          val df = spark.sql(query)
          val rows = df.collect()
          if (rows.isEmpty) empties += 1

          val reached = coverageOf(spark, query, targets)
          val fresh = reached -- covered
          covered ++= reached
          if (config.guided && fresh.nonEmpty) corpus += candidate
        } catch {
          case NonFatal(e) =>
            failures += FuzzFailure(i, s"${e.getClass.getSimpleName}: ${e.getMessage}", candidate)
            // an input that breaks the query is worth mutating further
            if (config.guided) corpus += candidate
        }
        i += 1
      }
    }

    FuzzResult(iterationsRun, failures.toSeq, covered.toSet, targets.map(_._2.size).sum, empties)
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

  /** One candidate input: `rowsPerTable` rows for every table. */
  private def generate(
      schemas: Map[String, StructType],
      pools: Map[(String, String), ValuePool],
      corpus: mutable.ArrayBuffer[Map[String, Seq[Row]]],
      config: FuzzConfig,
      random: Random): Map[String, Seq[Row]] = {

    val basis = corpus(random.nextInt(corpus.length))

    schemas.map { case (table, schema) =>
      val rows = (0 until config.rowsPerTable).map { _ =>
        val values = schema.fields.map { field =>
          value(table, field.name, field.dataType, pools, basis.get(table), schema,
            config.strategy, random)
        }
        Row.fromSeq(values.toIndexedSeq)
      }
      table -> rows
    }
  }

  /** One generated value, which is where the three strategies part company. */
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

      case MutationStrategy.Natural | MutationStrategy.CoDependent =>
        // pools already encode co-dependence when the strategy asked for it
        pools.get((table, column)) match {
          case Some(pool) if !pool.isEmpty => pool.pick(random)
          case _ =>
            // no observed values for this column: fall back rather than emit only nulls
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
        val conditions = DeSqlEngine.branchConditions(node.plan)
        if (conditions.isEmpty) None else Some(node.plan.children.head -> conditions)
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
