package org.bigasterisk.api

import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DistributionSuite extends AnyFunSuite with Matchers {

  private def draw(d: Distribution, n: Int = 2000, seed: Long = 1L): Seq[Any] = {
    val random = new Random(seed)
    (0 until n).map(_ => d.sample(random))
  }

  test("a discrete declaration yields only its own values") {
    val d = Distribution.parse("""Discrete("alice", "bob", "carol")""")
    d shouldBe Distribution.Discrete(IndexedSeq("alice", "bob", "carol"))
    draw(d, 200).toSet shouldBe Set("alice", "bob", "carol")
  }

  test("a discrete declaration accepts numbers as well as strings") {
    Distribution.parse("Discrete(1, 2, 3)") shouldBe
      Distribution.Discrete(IndexedSeq(1.0, 2.0, 3.0))
  }

  test("uniform stays inside its bounds") {
    val d = Distribution.parse("uniform(10, 20)")
    draw(d).foreach { v =>
      v.asInstanceOf[Double] should be >= 10.0
      v.asInstanceOf[Double] should be <= 20.0
    }
  }

  test("normal centres on its mean") {
    val values = draw(Distribution.parse("normal(170, 10)")).map(_.asInstanceOf[Double])
    (values.sum / values.size) shouldBe 170.0 +- 1.5
  }

  test("binomial stays within its trial count and centres on n*p") {
    val values = draw(Distribution.parse("binom(100, 0.1)")).map(_.asInstanceOf[Int])
    values.foreach { v => v should be >= 0; v should be <= 100 }
    (values.sum.toDouble / values.size) shouldBe 10.0 +- 1.0
  }

  test("poisson centres on lambda and is never negative") {
    val values = draw(Distribution.parse("poisson(4)")).map(_.asInstanceOf[Int])
    values.foreach(_ should be >= 0)
    (values.sum.toDouble / values.size) shouldBe 4.0 +- 0.6
  }

  test("zipf is skewed toward the first ranks") {
    val values = draw(Distribution.parse("zipf(50, 1.2)")).map(_.asInstanceOf[Int])
    values.foreach { v => v should be >= 1; v should be <= 50 }
    // the defining property: a few values dominate
    values.count(_ == 1) should be > values.count(_ == 50)
  }

  test("aliases are accepted and case is ignored") {
    Distribution.parse("BINOMIAL(10, 0.5)") shouldBe Distribution.Binomial(10, 0.5)
    Distribution.parse("Norm(0, 1)") shouldBe Distribution.Normal(0, 1)
    Distribution.parse("DISCRETE(\"a\")") shouldBe Distribution.Discrete(IndexedSeq("a"))
  }

  test("commas inside quotes do not split arguments") {
    Distribution.parse("""Discrete("a,b", "c")""") shouldBe
      Distribution.Discrete(IndexedSeq("a,b", "c"))
  }

  test("sampling is reproducible from a seed") {
    val d = Distribution.parse("normal(0, 1)")
    draw(d, 50, seed = 7L) shouldBe draw(d, 50, seed = 7L)
  }

  test("a malformed or unknown declaration is rejected by name") {
    val unknown = the[IllegalArgumentException] thrownBy Distribution.parse("gamma(1, 2)")
    unknown.getMessage should include("gamma")
    unknown.getMessage should include("binom")

    val malformed = the[IllegalArgumentException] thrownBy Distribution.parse("not a call")
    malformed.getMessage should include("malformed")

    an[IllegalArgumentException] should be thrownBy Distribution.parse("uniform(1)")
    an[IllegalArgumentException] should be thrownBy Distribution.parse("normal(a, b)")
  }

  test("nonsensical parameters are rejected") {
    an[IllegalArgumentException] should be thrownBy Distribution.parse("uniform(10, 1)")
    an[IllegalArgumentException] should be thrownBy Distribution.parse("binom(10, 2)")
    an[IllegalArgumentException] should be thrownBy Distribution.parse("binom(-1, 0.5)")
    an[IllegalArgumentException] should be thrownBy Distribution.parse("zipf(0, 1)")
    an[IllegalArgumentException] should be thrownBy Distribution.Discrete(IndexedSeq.empty)
  }

  test("a config reports which column a bad declaration belongs to") {
    val e = the[IllegalArgumentException] thrownBy
      TestGenConfig(distributions = Map("score" -> "gamma(1, 2)")).parsedDistributions
    e.getMessage should include("score")
  }

  test("declarations round-trip through their description") {
    Seq("uniform(1.0, 2.0)", "normal(0.0, 1.0)", "binom(10, 0.5)", "poisson(3.0)",
      "zipf(10, 1.5)").foreach { spec =>
      Distribution.parse(spec).description shouldBe spec
    }
  }
}
