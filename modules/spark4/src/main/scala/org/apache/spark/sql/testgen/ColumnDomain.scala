package org.apache.spark.sql.testgen

import scala.util.Random

import org.apache.spark.sql.types._

/**
 * The values one column may still take, given the constraints gathered so far.
 *
 * This is the solver. Full SMT is not needed for SQL filter predicates: a conjunction
 * of comparisons against literals is a set of interval and equality constraints per
 * column, and a witness can be read straight off the bounds. What this cannot express
 * — a constraint relating two columns, or arithmetic on the left-hand side — is
 * reported as unsupported rather than guessed at.
 *
 * @param lower       inclusive lower bound
 * @param upper       inclusive upper bound
 * @param equalTo     the value the column must take, if pinned
 * @param notEqualTo  values the column must avoid
 * @param mustBeNull  the column must be null
 * @param mustNotBeNull the column must not be null
 * @param prefix      the string the column must start with
 * @param contains    substrings the column must contain
 * @param unsatisfiable set when two constraints contradict
 */
case class ColumnDomain(
    dataType: DataType,
    lower: Option[Double] = None,
    upper: Option[Double] = None,
    equalTo: Option[Any] = None,
    notEqualTo: Set[Any] = Set.empty,
    mustBeNull: Boolean = false,
    mustNotBeNull: Boolean = false,
    prefix: Option[String] = None,
    contains: Seq[String] = Seq.empty,
    unsatisfiable: Boolean = false) {

  def contradiction: ColumnDomain = copy(unsatisfiable = true)

  /** True when no value can satisfy every constraint gathered. */
  def isUnsatisfiable: Boolean =
    unsatisfiable ||
      (mustBeNull && mustNotBeNull) ||
      (for (l <- lower; u <- upper) yield l > u).getOrElse(false) ||
      equalTo.exists(notEqualTo.contains)

  def withLower(v: Double, inclusive: Boolean): ColumnDomain = {
    val bound = if (inclusive) v else nextAbove(v)
    copy(lower = Some(lower.fold(bound)(math.max(_, bound))), mustNotBeNull = true)
  }

  def withUpper(v: Double, inclusive: Boolean): ColumnDomain = {
    val bound = if (inclusive) v else nextBelow(v)
    copy(upper = Some(upper.fold(bound)(math.min(_, bound))), mustNotBeNull = true)
  }

  /** Integral columns step by one; floating-point columns by a small epsilon. */
  private def nextAbove(v: Double): Double = dataType match {
    case IntegerType | LongType | ShortType | ByteType => v + 1
    case _                                             => v + 1e-6
  }

  private def nextBelow(v: Double): Double = dataType match {
    case IntegerType | LongType | ShortType | ByteType => v - 1
    case _                                             => v - 1e-6
  }

  /**
   * A value satisfying every constraint, or `None` if there is none.
   *
   * `natural` are values observed in real data. Preferring one of those that satisfies
   * the constraints is the whole of the naturalness idea: the path is the same, but the
   * record looks like a record instead of like solver output.
   */
  def witness(natural: IndexedSeq[Any], random: Random): Option[Any] = {
    if (isUnsatisfiable) return None
    if (mustBeNull) return Some(null)
    equalTo.foreach(v => return Some(v))

    natural.filter(satisfies).headOption.orElse(synthesise(random))
  }

  /** Whether a concrete value satisfies this domain. */
  def satisfies(value: Any): Boolean = {
    if (value == null) return mustBeNull || (!mustNotBeNull && lower.isEmpty && upper.isEmpty)
    if (mustBeNull) return false
    if (notEqualTo.contains(value)) return false
    if (equalTo.exists(_ != value)) return false

    val numericOk = value match {
      case n: Number =>
        val d = n.doubleValue()
        lower.forall(d >= _) && upper.forall(d <= _)
      case _ => lower.isEmpty && upper.isEmpty
    }
    val stringOk = value match {
      case s: String => prefix.forall(s.startsWith) && contains.forall(s.contains)
      case _         => prefix.isEmpty && contains.isEmpty
    }
    numericOk && stringOk
  }

  /** A value built to fit, when no observed value does. */
  private def synthesise(random: Random): Option[Any] = dataType match {
    case IntegerType | LongType | ShortType | ByteType | DoubleType | FloatType =>
      val candidate = (lower, upper) match {
        case (Some(l), Some(u)) => (l + u) / 2
        case (Some(l), None)    => l
        case (None, Some(u))    => u
        case (None, None)       => random.nextInt(100).toDouble
      }
      val snapped = dataType match {
        case IntegerType | LongType | ShortType | ByteType => math.ceil(candidate)
        case _                                             => candidate
      }
      val value = cast(snapped)
      if (satisfies(value)) Some(value) else None

    case StringType =>
      val base = prefix.getOrElse("") + contains.mkString
      val value = if (base.isEmpty) random.alphanumeric.take(6).mkString else base
      if (satisfies(value)) Some(value) else None

    case BooleanType =>
      Seq(true, false).find(satisfies)

    case _ => None
  }

  private def cast(d: Double): Any = dataType match {
    case IntegerType => d.toInt
    case LongType    => d.toLong
    case ShortType   => d.toShort
    case ByteType    => d.toByte
    case FloatType   => d.toFloat
    case _           => d
  }
}
