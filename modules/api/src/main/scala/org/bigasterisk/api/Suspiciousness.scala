package org.bigasterisk.api

/**
 * How suspicious an operation is, given how often it participated in producing wrong
 * results versus right ones.
 *
 * These are the standard spectrum-based fault-localisation formulas, applied to
 * dataflow operations rather than to lines of code.
 *
 * @group optdebug
 */
sealed trait Suspiciousness {

  /** A short name, used in reports. */
  def name: String

  /**
   * Scores one operation.
   *
   * @param failingHits  failing witnesses that passed through the operation
   * @param passingHits  passing witnesses that passed through the operation
   * @param totalFailing all failing witnesses
   * @param totalPassing all passing witnesses
   * @return a score in `[0, 1]`; higher is more suspicious
   */
  def score(failingHits: Long, passingHits: Long, totalFailing: Long, totalPassing: Long): Double
}

object Suspiciousness {

  /**
   * Ochiai.
   *
   * Rewards an operation that covers many failing witnesses, which makes it favour
   * operations that touch *every* record — an aggregation reaches all of them, so its
   * failing coverage is always maximal. Prefer [[Tarantula]] unless the failing witness
   * set has already been minimised down to the records actually responsible.
   */
  case object Ochiai extends Suspiciousness {
    val name = "ochiai"
    def score(ef: Long, ep: Long, totalF: Long, totalP: Long): Double = {
      val denominator = math.sqrt(totalF.toDouble * (ef + ep).toDouble)
      if (denominator == 0.0) 0.0 else ef.toDouble / denominator
    }
  }

  /**
   * Tarantula, the original spectrum formula, and the default here.
   *
   * It compares an operation's *rate* of failing coverage against its rate of passing
   * coverage, so an operation that touches every record scores a neutral 0.5 and one
   * that touches only failing records scores 1.0. That neutrality matters: a query's
   * aggregation reaches every witness of a wrong result, and should not out-rank the
   * branch of a `CASE WHEN` that only the culprit record took.
   */
  case object Tarantula extends Suspiciousness {
    val name = "tarantula"
    def score(ef: Long, ep: Long, totalF: Long, totalP: Long): Double = {
      val failRate = if (totalF == 0L) 0.0 else ef.toDouble / totalF.toDouble
      val passRate = if (totalP == 0L) 0.0 else ep.toDouble / totalP.toDouble
      if (failRate + passRate == 0.0) 0.0 else failRate / (failRate + passRate)
    }
  }

  val all: Seq[Suspiciousness] = Seq(Ochiai, Tarantula)
}

/**
 * One operation of a query, scored by how much it looks responsible for a wrong result.
 *
 * @param stepId            the operation's position in the query's decomposition
 * @param operator          the relational operator, e.g. `"Filter"`, `"Join"`
 * @param detail            the operator's expressions, as SQL text
 * @param branch            the conditional arm scored, when this row is a branch of the
 *                          operator rather than the operator as a whole
 * @param failingWitnesses  failing source records that reached this operation
 * @param passingWitnesses  passing source records that reached this operation
 * @param score             suspiciousness in `[0, 1]`; higher is more suspicious
 *
 * @group optdebug
 */
case class SuspiciousOperation(
    stepId: Int,
    operator: String,
    detail: String,
    branch: Option[String],
    failingWitnesses: Long,
    passingWitnesses: Long,
    score: Double) {

  /** True when this scores one arm of a conditional rather than the whole operator. */
  def isBranch: Boolean = branch.isDefined

  /** [[branch]] as a nullable string, for Java and Python callers. */
  def branchOrNull: String = branch.orNull

  override def toString: String = {
    val what = branch match {
      case Some(b) => s"$operator branch — $b"
      case None    => s"$operator${if (detail.isEmpty) "" else s" — $detail"}"
    }
    f"$score%.3f  [$stepId] $what  (failing=$failingWitnesses, passing=$passingWitnesses)"
  }
}
