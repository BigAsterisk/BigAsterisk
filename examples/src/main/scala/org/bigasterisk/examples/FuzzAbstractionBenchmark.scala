package org.bigasterisk.examples

import org.apache.spark.sql.SparkSession

import org.bigasterisk.api.{BigAsterisk, FuzzConfig}

/**
 * What framework abstraction is worth, measured rather than asserted.
 *
 * Runs the same fuzzing campaign twice — once evaluating each iteration through Spark,
 * once by interpreting the plan over in-memory rows — and reports the per-iteration
 * cost of each.
 *
 * {{{
 * bin/sbt 'examples/runMain org.bigasterisk.examples.FuzzAbstractionBenchmark'
 * bin/sbt 'examples/runMain org.bigasterisk.examples.FuzzAbstractionBenchmark 200'
 * }}}
 *
 * This is a benchmark rather than a test on purpose: the ratio depends on the machine,
 * and a suite that fails when a laptop is busy is a suite people learn to ignore. What
 * the suite pins instead is that both paths produce the same answer.
 */
object FuzzAbstractionBenchmark {

  private val Query =
    """SELECT c.name, SUM(o.amount) AS total
      |FROM orders o JOIN customers c ON o.cid = c.cid
      |WHERE o.amount > 100
      |GROUP BY c.name""".stripMargin

  def main(args: Array[String]): Unit = {
    val iterations = args.headOption.map(_.toInt).getOrElse(50)

    val spark = BigAsterisk
      .configure(
        SparkSession.builder()
          .master("local[2]")
          .appName("fuzz abstraction benchmark")
          .config("spark.ui.enabled", "false")
          .config("spark.sql.shuffle.partitions", "2"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val orders = spark.read.schema("oid STRING, cid STRING, amount INT")
      .csv("examples/data/orders.txt")
    val customers = spark.read.schema("cid STRING, name STRING")
      .csv("examples/data/customers.txt")
    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")

    val fuzzer = BigAsterisk.fuzz(spark)
    val seeds = Map("orders" -> orders, "customers" -> customers)

    // warm both paths so the measurement is of steady state, not of class loading
    Seq(true, false).foreach { abstracted =>
      fuzzer.fuzz(Query, seeds, FuzzConfig(iterations = 5, abstractFramework = abstracted))
    }

    def time(abstracted: Boolean): Long = {
      val started = System.nanoTime()
      fuzzer.fuzz(Query, seeds,
        FuzzConfig(iterations = iterations, seed = 1L, abstractFramework = abstracted))
      (System.nanoTime() - started) / 1000000
    }

    val onSpark = time(abstracted = false)
    val abstracted = time(abstracted = true)

    println()
    println(f"$iterations iterations of the same campaign:")
    println(f"  through Spark   $onSpark%6d ms   (${onSpark.toDouble / iterations}%6.1f ms per iteration)")
    println(f"  abstracted      $abstracted%6d ms   (${abstracted.toDouble / iterations}%6.1f ms per iteration)")
    println(f"  speedup         ${onSpark.toDouble / math.max(abstracted, 1L)}%6.1fx")
    println()

    spark.stop()
  }
}
