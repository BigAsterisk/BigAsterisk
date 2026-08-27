package org.apache.spark.sql.oracle

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BindReferences, Predicate}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation}
import org.apache.spark.sql.types.StructType

/**
 * An oracle written once as SQL, usable by every tool that needs one.
 *
 * The tools disagree about how an oracle is given: operation isolation and influence
 * take a SQL predicate, because they evaluate it inside the JVM against a DataFrame;
 * input isolation takes a `Row => Boolean`, because it re-runs the query over subsets
 * and tests each result on the driver.
 *
 * Writing each benchmark's oracle twice would let the two drift apart, and an oracle
 * that means two different things is worse than none. So it is written once as SQL and
 * compiled here — parsed by Spark's own parser, bound to the query's output schema, and
 * evaluated with Catalyst's own interpreter, so the function and the predicate cannot
 * disagree.
 */
object SqlOracle {

  /**
   * Compiles `predicate` — SQL over the output columns of `query` — into a function.
   *
   * Throws if the predicate names a column the query does not produce, which is a bug
   * in the benchmark rather than something to paper over at runtime.
   */
  def compile(spark: SparkSession, query: String, predicate: String): Row => Boolean = {
    compileFor(spark, spark.sql(query).schema, predicate)
  }

  /** [[compile]], against a schema already in hand. */
  def compileFor(
      spark: SparkSession,
      schema: StructType,
      predicate: String): Row => Boolean = {
    val attributes = schema.fields
      .map(f => AttributeReference(f.name, f.dataType, f.nullable)()).toIndexedSeq
    val parsed = CatalystSqlParser.parseExpression(predicate)

    // Resolved by Spark's own analyzer rather than by hand. Binding references is not
    // enough: `delta > 6000` over a double column needs a cast on the literal, and
    // without one the comparison fails at evaluation with a ClassCastException. The
    // analyzer is also what checks the predicate names columns that exist.
    val analyzer = spark.sessionState.analyzer
    val analyzed = analyzer.execute(Filter(parsed, LocalRelation(attributes)))
    analyzer.checkAnalysis(analyzed)
    val condition = analyzed match {
      case f: Filter => f.condition
      case other =>
        throw new IllegalArgumentException(s"'$predicate' is not a predicate: $other")
    }

    val bound = Predicate.createInterpreted(BindReferences.bindReference(condition, attributes))
    val toCatalyst = CatalystTypeConverters.createToCatalystConverter(schema)

    (row: Row) => bound.eval(toCatalyst(row).asInstanceOf[InternalRow])
  }
}
