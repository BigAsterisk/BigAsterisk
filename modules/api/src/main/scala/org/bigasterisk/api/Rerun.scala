package org.bigasterisk.api

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Running a query again with different data under its tables.
 *
 * This is the machinery behind every search-based tool here — fuzzing, symbolic test
 * generation, delta debugging — each of which works by proposing an input and asking
 * what the query does with it, over and over.
 *
 * Doing that is trivial for SQL text and not trivial for a DataFrame, which is why it
 * has an interface of its own. See [[Query]] for the difference; the short version is
 * that a DataFrame arrives already bound to the data it was built from, so substituting
 * means rewriting its plan — and rewriting plans is exactly the kind of thing that must
 * live behind the binding rather than in a tool.
 *
 * @group entry
 */
trait RerunSupport {

  /**
   * The query as a DataFrame reading `substitutions` in place of its own tables.
   *
   * Only the plan is rewritten. A query given as text resolves its tables through the
   * catalog and so reads whatever is registered at the moment it runs, which means a
   * caller using this directly is responsible for registering the substitutes and
   * putting the originals back. [[withData]] does both; prefer it.
   *
   * @param seeds the query's own tables, by the name it reads them under. These locate
   *              what to replace; for a query given as text the names are enough, and
   *              this may be empty.
   * @param substitutions what to read instead, by the same names
   */
  def substituted(
      spark: SparkSession,
      query: Query,
      seeds: Map[String, DataFrame],
      substitutions: Map[String, DataFrame]): DataFrame

  /** The query as a DataFrame over its own data. */
  def frame(spark: SparkSession, query: Query): DataFrame

  /**
   * Which of `seeds` this query could actually have data substituted for.
   *
   * A search tool that substitutes nothing still produces results, and they are all
   * about the original data — so it is worth being able to ask first. See
   * [[requireSubstitutable]].
   */
  def substitutable(query: Query, seeds: Map[String, DataFrame]): Set[String]

  /**
   * Runs `body` on the query reading `substitutions` in place of its own tables, and
   * puts the session back as it found it.
   */
  final def withData[A](
      spark: SparkSession,
      query: Query,
      seeds: Map[String, DataFrame],
      substitutions: Map[String, DataFrame])(body: DataFrame => A): A =
    Query.textOf(query) match {
      // Text resolves its tables by name, so the substitutes have to be registered
      // under those names and the originals put back afterwards.
      case Some(_) =>
        try {
          substitutions.foreach { case (name, df) => df.createOrReplaceTempView(name) }
          body(substituted(spark, query, seeds, substitutions))
        } finally {
          substitutions.keys.foreach { name =>
            seeds.get(name).foreach(_.createOrReplaceTempView(name))
          }
        }
      // A DataFrame is substituted in its plan; the session is never touched at all.
      case None => body(substituted(spark, query, seeds, substitutions))
    }

  /** Fails with an explanation if any seed is not substitutable. */
  final def requireSubstitutable(
      query: Query,
      seeds: Map[String, DataFrame],
      tool: String): Unit = {
    val missing = seeds.keySet -- substitutable(query, seeds)
    if (missing.nonEmpty) {
      throw new IllegalArgumentException(
        s"$tool cannot substitute data for ${missing.toSeq.sorted.mkString(", ")}: " +
        "the query's plan does not read them. When the query is a DataFrame, each seed " +
        "must be the DataFrame the pipeline was built from (or a table it reads under " +
        "that name), because substitution replaces that plan inside this one. " +
        s"Seeds given: ${seeds.keys.toSeq.sorted.mkString(", ")}.")
    }
  }

  /**
   * [[substituted]] for callers that cannot build a Scala `Map` — Java, and Py4J, which
   * marshals a Python dict to `java.util.Map` and has no static type for the query.
   */
  final def substitutedJava(
      spark: SparkSession,
      query: AnyRef,
      seeds: java.util.Map[String, DataFrame],
      substitutions: java.util.Map[String, DataFrame]): DataFrame =
    substituted(spark, Query.of(query), seeds.asScala.toMap, substitutions.asScala.toMap)
}
