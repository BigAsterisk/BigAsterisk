package org.bigasterisk.spark4

import org.apache.spark.sql.SparkSession

import org.bigasterisk.api.BigAsterisk

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * End-to-end check that the platform entry point resolves this module through
 * `ServiceLoader` and that provenance works when driven purely through the
 * version-independent API — no `org.apache.spark.sql.lineage` types in sight.
 */
class Spark4BindingSuite extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = BigAsterisk
      .configure(
        SparkSession.builder()
          .master("local[2]")
          .appName("Spark4BindingSuite")
          .config("spark.ui.enabled", "false")
          .config("spark.sql.shuffle.partitions", "2"))
      .getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  test("the binding is discovered on the classpath") {
    BigAsterisk.bindings.map(_.name) should contain("spark4")
  }

  test("the binding matches the Spark it was compiled against") {
    val binding = BigAsterisk.bindingFor(org.apache.spark.SPARK_VERSION)
    binding.name shouldBe "spark4"
    binding.versionRange.contains(
      org.bigasterisk.api.Version.parse(org.apache.spark.SPARK_VERSION)) shouldBe true
  }

  test("configure registers the SQL extension so validate passes") {
    noException should be thrownBy BigAsterisk.binding(spark)
  }

  test("a session built without the extension is rejected with actionable guidance") {
    val e = the[IllegalStateException] thrownBy Spark4Binding.checkExtensions(None)
    e.getMessage should include("BigAsterisk.configure")
    e.getMessage should include("spark.sql.extensions")
    e.getMessage should include("Currently set: <unset>")

    // another project's extension present, ours absent
    val other = the[IllegalStateException] thrownBy
      Spark4Binding.checkExtensions(Some("com.example.OtherExtension"))
    other.getMessage should include("com.example.OtherExtension")

    // ours alongside another is accepted
    noException should be thrownBy Spark4Binding.checkExtensions(
      Some(s"com.example.OtherExtension,${Spark4Binding.extensionClassName}"))
  }

  test("provenance round-trips through the version-independent API") {
    val session = spark
    val lineage = BigAsterisk.lineage(session)
    lineage.enableCapture(session)

    // A real file source: capture needs a scan it can deterministically re-read.
    // An in-memory LocalTableScan has no pre-exchange tap and is refused outright,
    // which is the fail-loud coverage policy working as designed.
    session.read
      .option("header", "false")
      .schema("category STRING, amount INT")
      .csv("src/test/resources/sales.txt")
      .createOrReplaceTempView("sales_api_suite")

    val df = session.sql(
      "SELECT category, SUM(amount) AS total FROM sales_api_suite GROUP BY category")

    val out = lineage.collectWithLineage(df)
    out should not be empty
    val electronics = out.find(_._1.getString(0) == "electronics")
    electronics shouldBe defined

    val cursor = lineage.trace(df, Seq(electronics.get._2))
    cursor.ids should contain(electronics.get._2)

    // walk back to the scan; every witness must be an "electronics" row, and they
    // must sum to the aggregate the trace started from
    var c = cursor
    var guard = 0
    while (!c.atScan && guard < 10) { c = c.goBack(); guard += 1 }
    c.atScan shouldBe true

    val witnesses = c.show(full = true)
    witnesses should not be empty
    witnesses.map(_.getString(0)).distinct shouldBe Array("electronics")
    witnesses.map(_.getInt(1).toLong).sum shouldBe electronics.get._1.getLong(1)

    // backward() is the same walk in one call
    lineage.backward(df, Seq(electronics.get._2)).length shouldBe witnesses.length

    lineage.releaseLineage(df)
  }
}
