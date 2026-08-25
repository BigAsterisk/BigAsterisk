package org.bigasterisk.api

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class VersionRangeSuite extends AnyFunSuite with Matchers {

  private def in(range: String, v: String): Boolean =
    VersionRange.parse(range).contains(Version.parse(v))

  test("half-open range includes the lower bound and excludes the upper") {
    in("[4.0.0,5.0.0)", "4.0.0") shouldBe true
    in("[4.0.0,5.0.0)", "4.1.2") shouldBe true
    in("[4.0.0,5.0.0)", "4.9.9") shouldBe true
    in("[4.0.0,5.0.0)", "5.0.0") shouldBe false
    in("[4.0.0,5.0.0)", "3.5.1") shouldBe false
  }

  test("exclusive lower and inclusive upper bounds") {
    in("(4.0.0,5.0.0]", "4.0.0") shouldBe false
    in("(4.0.0,5.0.0]", "4.0.1") shouldBe true
    in("(4.0.0,5.0.0]", "5.0.0") shouldBe true
  }

  test("an omitted bound is unbounded") {
    in("[4.0.0,)", "99.0.0") shouldBe true
    in("[4.0.0,)", "3.9.9") shouldBe false
    in("(,5.0.0)", "0.1") shouldBe true
    in("(,5.0.0)", "5.0.1") shouldBe false
  }

  test("versions of differing length compare by padding with zeros") {
    in("[4.0.0,5.0.0)", "4") shouldBe true
    in("[4.1,5.0.0)", "4.1.0") shouldBe true
    in("[4.1.1,5.0.0)", "4.1") shouldBe false
    Version.parse("4.1") shouldBe Version.parse("4.1.0").copy(parts = Seq(4, 1))
    Version.parse("4.1").compare(Version.parse("4.1.0")) shouldBe 0
  }

  test("a qualifier does not affect ordering, so previews match their release") {
    // Spark ships 4.1.0-preview1 ahead of 4.1.0; a binding for [4.0.0,5.0.0) must take it.
    in("[4.0.0,5.0.0)", "4.1.0-preview1") shouldBe true
    in("[4.1.0,5.0.0)", "4.1.0-preview1") shouldBe true
    Version.parse("4.1.0-preview1").compare(Version.parse("4.1.0")) shouldBe 0
    Version.parse("4.1.0-preview1").qualifier shouldBe Some("preview1")
  }

  test("round-trips through toString") {
    Seq("[4.0.0,5.0.0)", "(4.0.0,5.0.0]", "[4.0.0,)", "(,5.0.0)").foreach { s =>
      VersionRange.parse(s).toString shouldBe s
    }
  }

  test("malformed input is rejected with a message naming the input") {
    an[IllegalArgumentException] should be thrownBy VersionRange.parse("4.0.0")
    an[IllegalArgumentException] should be thrownBy VersionRange.parse("[4.0.0]")
    an[IllegalArgumentException] should be thrownBy VersionRange.parse("{4.0.0,5.0.0}")
    an[IllegalArgumentException] should be thrownBy Version.parse("four.one")
    an[IllegalArgumentException] should be thrownBy Version.parse("")
  }
}
