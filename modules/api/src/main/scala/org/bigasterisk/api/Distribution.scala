package org.bigasterisk.api

import scala.util.Random

/**
 * A declared distribution for a column's values.
 *
 * The insight this exists for is that a developer usually knows the shape of their own
 * data — that a zip code is one of a known set, that a height clusters around a mean,
 * that a score is binomial. A solver does not, and left to itself will satisfy
 * `age > 18` with `19` every time. Told the distribution, it can satisfy the same
 * constraint with a value that looks like it came from the real table.
 *
 * Declarations are written as text, close to the notation the technique's own input
 * annotations use:
 *
 * {{{
 * Map(
 *   "name"   -> """Discrete("alice", "bob", "carol")""",
 *   "score"  -> "binom(100, 0.1)",
 *   "height" -> "normal(170, 10)",
 *   "amount" -> "uniform(0, 500)")
 * }}}
 *
 * @group testgen
 */
sealed trait Distribution {

  /** A value drawn from this distribution. */
  def sample(random: Random): Any

  /** How it was written, for reporting. */
  def description: String

  override def toString: String = description
}

object Distribution {

  /** One of a fixed set of values, uniformly. */
  final case class Discrete(values: IndexedSeq[Any]) extends Distribution {
    require(values.nonEmpty, "Discrete needs at least one value")
    def sample(random: Random): Any = values(random.nextInt(values.length))
    def description: String = values.mkString("Discrete(", ", ", ")")
  }

  /** Uniform over `[low, high]`. */
  final case class Uniform(low: Double, high: Double) extends Distribution {
    require(high >= low, s"uniform needs high >= low, got ($low, $high)")
    def sample(random: Random): Any = low + random.nextDouble() * (high - low)
    def description: String = s"uniform($low, $high)"
  }

  /** Normal with the given mean and standard deviation. */
  final case class Normal(mean: Double, sd: Double) extends Distribution {
    require(sd >= 0, s"normal needs sd >= 0, got $sd")
    def sample(random: Random): Any = mean + random.nextGaussian() * sd
    def description: String = s"normal($mean, $sd)"
  }

  /** Number of successes in `n` trials of probability `p`. */
  final case class Binomial(n: Int, p: Double) extends Distribution {
    require(n >= 0, s"binom needs n >= 0, got $n")
    require(p >= 0.0 && p <= 1.0, s"binom needs 0 <= p <= 1, got $p")
    def sample(random: Random): Any = (0 until n).count(_ => random.nextDouble() < p)
    def description: String = s"binom($n, $p)"
  }

  /** Poisson with the given mean, by Knuth's method. */
  final case class Poisson(lambda: Double) extends Distribution {
    require(lambda >= 0, s"poisson needs lambda >= 0, got $lambda")
    def sample(random: Random): Any = {
      val limit = math.exp(-lambda)
      var k = 0
      var product = random.nextDouble()
      while (product > limit) { k += 1; product *= random.nextDouble() }
      k
    }
    def description: String = s"poisson($lambda)"
  }

  /**
   * Zipf over the ranks `1..n` with exponent `s`.
   *
   * The distribution of a skewed key — a few values that dominate and a long tail —
   * which is what makes a generated join or grouping behave like a real one.
   */
  final case class Zipf(n: Int, s: Double) extends Distribution {
    require(n > 0, s"zipf needs n > 0, got $n")
    require(s > 0, s"zipf needs s > 0, got $s")
    private val weights: IndexedSeq[Double] = (1 to n).map(k => 1.0 / math.pow(k, s))
    private val total: Double = weights.sum
    def sample(random: Random): Any = {
      val target = random.nextDouble() * total
      var accumulated = 0.0
      var rank = 0
      while (rank < n - 1 && accumulated + weights(rank) < target) {
        accumulated += weights(rank); rank += 1
      }
      rank + 1
    }
    def description: String = s"zipf($n, $s)"
  }

  private val Call = """(?s)\s*([A-Za-z]+)\s*\((.*)\)\s*""".r

  /**
   * Parses a declaration.
   *
   * Accepts `Discrete("a", "b")`, `uniform(lo, hi)`, `normal(mean, sd)`,
   * `binom(n, p)`, `poisson(lambda)` and `zipf(n, s)`, case-insensitively.
   *
   * @throws IllegalArgumentException naming the forms that are understood, since a
   *         typo in a declaration would otherwise be silently ignored and the tests
   *         would quietly stop looking like the data.
   */
  def parse(spec: String): Distribution = spec match {
    case Call(name, argsText) =>
      val args = splitArgs(argsText)
      name.toLowerCase match {
        case "discrete" =>
          Discrete(args.map(unquote).toIndexedSeq)
        case "uniform" =>
          val Seq(lo, hi) = numbers(name, args, 2); Uniform(lo, hi)
        case "normal" | "gauss" | "norm" =>
          val Seq(mean, sd) = numbers(name, args, 2); Normal(mean, sd)
        case "binom" | "binomial" =>
          val Seq(n, p) = numbers(name, args, 2); Binomial(n.toInt, p)
        case "poisson" =>
          val Seq(lambda) = numbers(name, args, 1); Poisson(lambda)
        case "zipf" =>
          val Seq(n, s) = numbers(name, args, 2); Zipf(n.toInt, s)
        case other =>
          throw new IllegalArgumentException(
            s"unknown distribution '$other' in '$spec'; expected one of " +
              "Discrete, uniform, normal, binom, poisson, zipf")
      }
    case _ =>
      throw new IllegalArgumentException(
        s"malformed distribution '$spec'; expected something like " +
          """Discrete("a", "b") or binom(100, 0.1)""")
  }

  /** Splits on commas that are not inside quotes. */
  private def splitArgs(text: String): Seq[String] = {
    val out = List.newBuilder[String]
    val current = new StringBuilder
    var quoted = false
    text.foreach {
      case '"' => quoted = !quoted; current += '"'
      case ',' if !quoted => out += current.toString.trim; current.clear()
      case c => current += c
    }
    val last = current.toString.trim
    if (last.nonEmpty) out += last
    out.result().filter(_.nonEmpty)
  }

  private def unquote(s: String): Any = {
    val t = s.trim
    if (t.length >= 2 && t.startsWith("\"") && t.endsWith("\"")) t.substring(1, t.length - 1)
    else t.toDoubleOption.getOrElse(t)
  }

  private def numbers(name: String, args: Seq[String], expected: Int): Seq[Double] = {
    require(args.length == expected,
      s"$name takes $expected argument(s), got ${args.length}: ${args.mkString(", ")}")
    args.map { a =>
      a.trim.toDoubleOption.getOrElse(
        throw new IllegalArgumentException(s"$name expects numbers, got '$a'"))
    }
  }
}
