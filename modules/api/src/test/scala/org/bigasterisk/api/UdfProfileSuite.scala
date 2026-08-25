package org.bigasterisk.api

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The model the Python analyser fills in, and the registry the engines read. */
class UdfProfileSuite extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  override def afterEach(): Unit = UdfRegistry.clear()

  private val classify = Seq(
    "udf\tclassify\tamount",
    "branch\tamount > 1000\tamount",
    "branch\tamount > 100\tamount",
    "path\tamount > 1000\t'high'\texact",
    "path\t(NOT amount > 1000) AND amount > 100\t'medium'\texact",
    "path\t(NOT amount > 1000) AND (NOT amount > 100)\t'low'\texact",
    "influence\tamount")

  test("a profile is read back from the line format") {
    val profile = UdfProfile.parse(classify)

    profile.name shouldBe "classify"
    profile.parameters shouldBe Seq("amount")
    profile.branches.map(_.condition) shouldBe Seq("amount > 1000", "amount > 100")
    profile.branches.head.parameters shouldBe Set("amount")
    profile.paths should have size 3
    profile.influencing shouldBe Set("amount")
    profile.isComplete shouldBe true
    profile.isSolvable shouldBe true
  }

  test("paths are matched to the value they produce") {
    val profile = UdfProfile.parse(classify)

    profile.pathsYielding("'high'").map(_.constraint) shouldBe Seq("amount > 1000")
    profile.pathsYielding("'medium'").head.constraint shouldBe
      "(NOT amount > 1000) AND amount > 100"
    profile.pathsYielding("'nonexistent'") shouldBe empty
  }

  test("a function the analysis could not fully read says so") {
    val profile = UdfProfile.parse(Seq(
      "udf\tmystery\ts",
      "path\ttrue\t1\tapproximate",
      "unsupported\tmethod 'encode' is not understood"))

    profile.isComplete shouldBe false
    profile.isSolvable shouldBe false
    // an inexact path must never stand in for the call it came from
    profile.pathsYielding("1") shouldBe empty
    profile.toString should include("partial (1 unread)")
  }

  test("a parameter that cannot reach the result is not reported as influencing") {
    // `def pick(a, b): return a` — b is passed and ignored
    val profile = UdfProfile.parse(Seq(
      "udf\tpick\ta,b", "path\ttrue\t\texact", "influence\ta"))

    profile.influences(0) shouldBe true
    profile.influences(1) shouldBe false
    profile.influences(9) shouldBe false
  }

  test("a profile with several parameters keeps their order") {
    val profile = UdfProfile.parse(Seq("udf\tf\tfirst,second,third", "path\ttrue\t\texact"))
    profile.parameters shouldBe Seq("first", "second", "third")
  }

  test("a malformed profile is rejected rather than half-read") {
    an[IllegalArgumentException] should be thrownBy UdfProfile.parse(Seq("branch\tx > 1\tx"))
    an[IllegalArgumentException] should be thrownBy UdfProfile.parse(Seq("udf\tf\ta", "nonsense\tx"))
    an[IllegalArgumentException] should be thrownBy UdfProfile.parse(Seq("udf\tf\ta", "path\tx > 1"))
    an[IllegalArgumentException] should be thrownBy UdfProfile(name = "", parameters = Nil)
  }

  test("blank lines are ignored, and a function with no branches is still a profile") {
    val profile = UdfProfile.parse(Seq("udf\tdouble\tx", "", "path\ttrue\t\texact", "influence\tx"))
    profile.branches shouldBe empty
    profile.paths.head.constraint shouldBe "true"
    profile.paths.head.returns shouldBe empty
    profile.isSolvable shouldBe true
  }

  test("the registry is empty until something is registered") {
    UdfRegistry.size shouldBe 0
    UdfRegistry.lookup("classify") shouldBe empty
    UdfRegistry.names shouldBe empty
  }

  test("registering makes a profile findable by the name a query uses") {
    UdfRegistry.registerLines(classify.toArray) should include("classify")

    UdfRegistry.lookup("classify").map(_.parameters) shouldBe Some(Seq("amount"))
    UdfRegistry.names shouldBe Set("classify")
    UdfRegistry.size shouldBe 1
  }

  test("registering the same name again replaces the profile") {
    UdfRegistry.registerLines(classify.toArray)
    UdfRegistry.registerLines(Array("udf\tclassify\tx,y", "path\ttrue\t\texact"))

    UdfRegistry.size shouldBe 1
    UdfRegistry.lookup("classify").map(_.parameters) shouldBe Some(Seq("x", "y"))
  }

  test("a profile can be forgotten") {
    UdfRegistry.registerLines(classify.toArray)
    UdfRegistry.remove("classify")
    UdfRegistry.lookup("classify") shouldBe empty

    UdfRegistry.registerLines(classify.toArray)
    UdfRegistry.clear()
    UdfRegistry.size shouldBe 0
  }
}
