package org.bigasterisk.api

import scala.language.implicitConversions

import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * Something the tools can run more than once, against whatever its input tables hold
 * at the time.
 *
 * Several of the tools here are search procedures: they substitute data under the
 * query's tables and run it again, dozens or hundreds of times. Fuzzing generates the
 * substitute; test generation solves for it; delta debugging removes rows from it. What
 * they all need is not a result but a *recipe* — something that will read whatever is
 * under `flights` the next time it is asked.
 *
 * SQL text is such a recipe already: re-parsing it picks up whatever the name now
 * resolves to. A DataFrame is not. By the time you hold one, its plan is analysed and
 * bound: `flights` has become a particular relation with particular attribute ids, and
 * running it again runs it against the same data. That is why the tools originally took
 * the query as text.
 *
 * Requiring text is the wrong trade. It says that to debug a DataFrame pipeline you
 * must first rewrite it as a SQL string — which is not the program you are debugging.
 * So a [[Query]] is either, and the binding is responsible for making a DataFrame
 * re-runnable by substituting into its plan (see the binding's plan rebinding: seed
 * plans are matched inside the query's plan and replaced, with the original output
 * attributes preserved so everything above them still refers to the right columns).
 *
 * Both forms convert implicitly, so `fuzz("SELECT ...", seeds)` and `fuzz(df, seeds)`
 * are both just calls.
 *
 * @group entry
 */
sealed trait Query {

  /** How to describe this query in a message, briefly. */
  def describe: String
}

/** @group entry */
object Query {

  /** A query given as SQL text, run by parsing it against the current catalog. */
  final case class Sql(text: String) extends Query {
    override def describe: String = text.replaceAll("\\s+", " ").trim
  }

  /**
   * A query given as a DataFrame, run by substituting into its analysed plan.
   *
   * @param df the pipeline, built however you build pipelines
   */
  final case class Frame(df: DataFrame) extends Query {
    override def describe: String = df.queryExecution.analyzed.nodeName + " ..."
  }

  implicit def fromText(text: String): Query = Sql(text)

  implicit def fromFrame(df: DataFrame): Query = Frame(df)

  /** The query as text, if it was given as text. */
  def textOf(query: Query): Option[String] = query match {
    case Sql(text) => Some(text)
    case _ => None
  }

  /**
   * A [[Query]] from whatever a foreign caller handed over.
   *
   * The implicit conversions above are a compile-time convenience and so are no use to
   * Java or to Py4J, which arrives holding a `String` or a `Dataset` and no static type
   * at all. This is the same choice made at run time.
   */
  def of(query: AnyRef): Query = query match {
    case q: Query => q
    case text: String => Sql(text)
    // `DataFrame` is `Dataset[Row]`, and erasure leaves no `Row` to test for; a
    // `Dataset` of anything else would fail later, and more obscurely, than here.
    case ds: Dataset[_] => Frame(ds.toDF())
    case other =>
      throw new IllegalArgumentException(
        "a query must be SQL text or a DataFrame, not " +
        (if (other == null) "null" else other.getClass.getName))
  }
}
