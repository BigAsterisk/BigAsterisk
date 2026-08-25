package org.bigasterisk.api

/**
 * A Maven-style version range used by a [[SparkBinding]] to declare which Spark
 * releases it supports, e.g. `"[4.0.0,5.0.0)"`.
 *
 * Square brackets are inclusive, parentheses exclusive; either bound may be empty
 * to mean unbounded (`"[4.0.0,)"`). Versions compare component-wise on their
 * leading numeric parts, so qualifiers such as `4.1.0-preview1` compare equal to
 * `4.1.0` — a binding that supports a release also supports its previews.
 *
 * @group spi
 */
final case class VersionRange(
    lower: Option[Version],
    lowerInclusive: Boolean,
    upper: Option[Version],
    upperInclusive: Boolean) {

  def contains(v: Version): Boolean = {
    val okLower = lower.forall { l =>
      val c = v.compare(l); if (lowerInclusive) c >= 0 else c > 0
    }
    val okUpper = upper.forall { u =>
      val c = v.compare(u); if (upperInclusive) c <= 0 else c < 0
    }
    okLower && okUpper
  }

  override def toString: String = {
    val lb = if (lowerInclusive) "[" else "("
    val rb = if (upperInclusive) "]" else ")"
    s"$lb${lower.map(_.toString).getOrElse("")},${upper.map(_.toString).getOrElse("")}$rb"
  }
}

object VersionRange {

  /** Parses `"[4.0.0,5.0.0)"`. Throws [[IllegalArgumentException]] on malformed input. */
  def parse(s: String): VersionRange = {
    val t = s.trim
    require(t.length >= 3, s"malformed version range: '$s'")
    val lowerInclusive = t.head match {
      case '[' => true
      case '(' => false
      case c   => throw new IllegalArgumentException(s"version range must start with '[' or '(': '$s' (found '$c')")
    }
    val upperInclusive = t.last match {
      case ']' => true
      case ')' => false
      case c   => throw new IllegalArgumentException(s"version range must end with ']' or ')': '$s' (found '$c')")
    }
    val body = t.substring(1, t.length - 1)
    val comma = body.indexOf(',')
    require(comma >= 0, s"version range must contain a comma: '$s'")
    val lo = body.substring(0, comma).trim
    val hi = body.substring(comma + 1).trim
    VersionRange(
      if (lo.isEmpty) None else Some(Version.parse(lo)), lowerInclusive,
      if (hi.isEmpty) None else Some(Version.parse(hi)), upperInclusive)
  }
}

/**
 * A dotted numeric version. Only the leading numeric components are significant;
 * a trailing qualifier (`-preview1`, `-SNAPSHOT`) is recorded but ignored when
 * comparing, so previews match the release they lead up to.
 *
 * @group spi
 */
final case class Version(parts: Seq[Int], qualifier: Option[String]) extends Ordered[Version] {

  def compare(that: Version): Int = {
    val n = math.max(parts.length, that.parts.length)
    var i = 0
    while (i < n) {
      val a = if (i < parts.length) parts(i) else 0
      val b = if (i < that.parts.length) that.parts(i) else 0
      if (a != b) return java.lang.Integer.compare(a, b)
      i += 1
    }
    0
  }

  override def toString: String =
    parts.mkString(".") + qualifier.map("-" + _).getOrElse("")
}

object Version {

  /** Parses `"4.1.2"`, `"4.1.0-preview1"`, `"4.1"`. Non-numeric leading input throws. */
  def parse(s: String): Version = {
    val t = s.trim
    require(t.nonEmpty, "empty version string")
    val dash = t.indexOf('-')
    val (numeric, qual) =
      if (dash >= 0) (t.substring(0, dash), Some(t.substring(dash + 1))) else (t, None)
    val parts = numeric.split('.').toSeq.map { p =>
      try p.toInt
      catch {
        case _: NumberFormatException =>
          throw new IllegalArgumentException(s"non-numeric version component '$p' in '$s'")
      }
    }
    require(parts.nonEmpty, s"no version components in '$s'")
    Version(parts, qual)
  }
}
