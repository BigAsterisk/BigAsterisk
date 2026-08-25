package org.apache.spark.sql.fuzz

import scala.util.Random

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * The values a generated column may take.
 *
 * A pool is what separates the three fuzzers. Drawing from a column's own observed
 * values gives rows that look real; drawing from a *shared* pool for two columns joined
 * together gives rows that still match after the join; drawing from nothing gives the
 * random baseline.
 */
case class ValuePool(values: IndexedSeq[Any]) {
  def isEmpty: Boolean = values.isEmpty
  def pick(random: Random): Any = if (isEmpty) null else values(random.nextInt(values.length))
}

object ValuePool {

  /** How many distinct values to remember per column. */
  val MaxValues: Int = 1000

  /** The observed values of each column of `rows`, capped at [[MaxValues]]. */
  def of(rows: Seq[Row], schema: StructType): Map[String, ValuePool] =
    schema.fields.map { field =>
      val index = schema.fieldIndex(field.name)
      val observed = rows.iterator
        .map(r => if (r.isNullAt(index)) null else r.get(index))
        .distinct
        .take(MaxValues)
        .toIndexedSeq
      field.name -> ValuePool(observed)
    }.toMap

  /**
   * Values chosen to provoke a reaction rather than to look plausible: boundaries,
   * empties, and the values that break naive parsing and arithmetic.
   */
  def boundaryValues(dataType: DataType): IndexedSeq[Any] = dataType match {
    case IntegerType => IndexedSeq(0, 1, -1, Int.MaxValue, Int.MinValue)
    case LongType    => IndexedSeq(0L, 1L, -1L, Long.MaxValue, Long.MinValue)
    case ShortType   => IndexedSeq(0.toShort, (-1).toShort, Short.MaxValue, Short.MinValue)
    case ByteType    => IndexedSeq(0.toByte, (-1).toByte, Byte.MaxValue, Byte.MinValue)
    case DoubleType =>
      IndexedSeq(0.0, -1.0, 1.0, Double.MaxValue, Double.MinValue,
        Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity)
    case FloatType   => IndexedSeq(0.0f, -1.0f, Float.MaxValue, Float.NaN)
    case StringType  => IndexedSeq("", " ", "0", "-1", "null", "a" * 256)
    case BooleanType => IndexedSeq(true, false)
    case _           => IndexedSeq.empty
  }

  /** A value for `dataType`, drawn without reference to any observed data. */
  def randomValue(dataType: DataType, random: Random): Any = dataType match {
    case IntegerType => random.nextInt()
    case LongType    => random.nextLong()
    case ShortType   => random.nextInt().toShort
    case ByteType    => random.nextInt().toByte
    case DoubleType  => random.nextDouble() * random.nextInt()
    case FloatType   => random.nextFloat() * random.nextInt()
    case StringType  => random.alphanumeric.take(1 + random.nextInt(12)).mkString
    case BooleanType => random.nextBoolean()
    case _           => null
  }
}
