package org.apache.spark.sql.testgen

import scala.util.Random

import org.apache.spark.sql.types._

import org.bigasterisk.api.Distribution

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
   * Three sources, in order of how much they know about the data:
   *
   *   1. a **declared distribution**, if the caller gave one for this column — the
   *      developer's own knowledge of the shape of their data;
   *   2. a value **observed** in the seed data;
   *   3. a value **synthesised** from the bounds.
   *
   * Each is only used if it satisfies the path, so naturalness never costs coverage: a
   * declared distribution that cannot reach the constraint falls through to a value
   * that can.
   */
  def witness(
      natural: IndexedSeq[Any],
      random: Random,
      distribution: Option[Distribution] = None): Option[Any] = {
    if (isUnsatisfiable) return None
    if (mustBeNull) return Some(null)
    equalTo.foreach(v => return Some(v))

    distribution.flatMap(sampleFrom(_, random)).orElse {
      // Any observed value that fits will do, and picking among them at random keeps
      // generated rows from all being identical when several tests share a path.
      val fits = natural.filter(satisfies)
      if (fits.nonEmpty) Some(fits(random.nextInt(fits.length))) else synthesise(random)
    }
  }

  /**
   * Draws from `distribution` until a value fits, giving up after a bounded number of
   * attempts.
   *
   * A declared distribution and a path constraint can simply be incompatible —
   * `binom(100, 0.1)` cannot produce a value above 100 — and in that case the caller
   * falls back rather than looping.
   */
  private def sampleFrom(distribution: Distribution, random: Random): Option[Any] = {
    var attempt = 0
    while (attempt < ColumnDomain.SamplingAttempts) {
      val drawn = coerce(distribution.sample(random))
      if (satisfies(drawn)) return Some(drawn)
      attempt += 1
    }
    None
  }

  /** Brings a sampled value to this column's type. */
  private def coerce(value: Any): Any = (value, dataType) match {
    case (n: Number, IntegerType) => n.intValue()
    case (n: Number, LongType)    => n.longValue()
    case (n: Number, ShortType)   => n.shortValue()
    case (n: Number, ByteType)    => n.byteValue()
    case (n: Number, DoubleType)  => n.doubleValue()
    case (n: Number, FloatType)   => n.floatValue()
    case (n: Number, StringType)  => n.toString
    case (other, _)               => other
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

object ColumnDomain {

  /**
   * How many draws to take from a declared distribution before falling back.
   *
   * A declared distribution and a path constraint can simply be incompatible, so this
   * bounds the attempt rather than looping: naturalness is preferred, never required.
   */
  val SamplingAttempts: Int = 64
}
