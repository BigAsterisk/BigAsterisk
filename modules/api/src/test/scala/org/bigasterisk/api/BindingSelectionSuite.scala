package org.bigasterisk.api

import org.apache.spark.sql.SparkSession

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BindingSelectionSuite extends AnyFunSuite with Matchers {

  /** A binding that implements only what selection needs. */
  private class FakeBinding(val name: String, val sparkVersions: String) extends SparkBinding {
    def requiredConf: Map[String, String] = Map.empty
    def lineage: LineageSupport = throw new UnsupportedOperationException
    def desql: DeSqlSupport = throw new UnsupportedOperationException
    def watchpoints: WatchpointSupport = throw new UnsupportedOperationException
    def vega: VegaSupport = throw new UnsupportedOperationException
    def validate(spark: SparkSession): Unit = ()
  }

  private val spark4 = new FakeBinding("spark4", "[4.0.0,5.0.0)")
  private val spark5 = new FakeBinding("spark5", "[5.0.0,6.0.0)")

  test("picks the binding whose range covers the running Spark") {
    BigAsterisk.select(Seq(spark4, spark5), "4.1.2").name shouldBe "spark4"
    BigAsterisk.select(Seq(spark4, spark5), "5.2.0").name shouldBe "spark5"
  }

  test("selection is independent of registration order") {
    BigAsterisk.select(Seq(spark5, spark4), "4.1.2").name shouldBe "spark4"
  }

  test("a Spark preview resolves to the binding for its release line") {
    BigAsterisk.select(Seq(spark4, spark5), "5.0.0-preview1").name shouldBe "spark5"
  }

  test("a more specific binding wins over a broader fallback") {
    val fallback = new FakeBinding("fallback", "[4.0.0,)")
    val pinned = new FakeBinding("spark4.1", "[4.1.0,4.2.0)")
    BigAsterisk.select(Seq(fallback, pinned), "4.1.2").name shouldBe "spark4.1"
    // outside the pinned range the fallback still applies
    BigAsterisk.select(Seq(fallback, pinned), "4.3.0").name shouldBe "fallback"
  }

  test("an empty classpath reports the Spark version and what to install") {
    val e = the[IllegalStateException] thrownBy BigAsterisk.select(Seq.empty, "4.1.2")
    e.getMessage should include("4.1.2")
    e.getMessage should include("bigasterisk-spark4")
  }

  test("an unsupported Spark version lists the bindings that were found") {
    val e = the[IllegalStateException] thrownBy BigAsterisk.select(Seq(spark4, spark5), "3.5.1")
    e.getMessage should include("Spark 3.5.1")
    e.getMessage should include("spark4 supports [4.0.0,5.0.0)")
    e.getMessage should include("spark5 supports [5.0.0,6.0.0)")
  }
}
