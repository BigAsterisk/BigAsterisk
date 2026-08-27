package org.bigasterisk.benchmarks

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.oracle.SqlOracle
import org.apache.spark.sql.types.StructType

/**
 * A benchmark's oracle.
 *
 * The compiling half lives in [[org.apache.spark.sql.oracle.SqlOracle]], because every
 * tool that takes an oracle needs it — not only the benchmarks. What is here is the part
 * that is specific to a fault-injection evaluation: deciding wrongness by difference.
 */
object Oracle {

  /** Compiles `predicate` — SQL over the output columns of `query` — into a function. */
  def compile(spark: SparkSession, query: String, predicate: String): Row => Boolean =
    SqlOracle.compile(spark, query, predicate)

  /** [[compile]], against a schema already in hand. */
  def compileFor(spark: SparkSession, schema: StructType, predicate: String): Row => Boolean =
    SqlOracle.compileFor(spark, schema, predicate)

  /**
   * An oracle for the mutated-program fault model: a row is wrong when the correct
   * program does not produce it.
   *
   * A fault-injection evaluation defines wrongness by difference, not by a threshold —
   * and a threshold would have to be recalibrated for every program and every input
   * size, which is how an oracle silently stops testing anything.
   */
  def differential(spark: SparkSession, correctQuery: String): Row => Boolean = {
    val correct = spark.sql(correctQuery).collect().map(_.toSeq).toSet
    (row: Row) => !correct.contains(row.toSeq)
  }
}
