package org.bigasterisk.benchmarks

import scala.util.Random

import org.apache.spark.sql.Row

/**
 * The subject programs, ported from the upstream artifacts.
 *
 * Each is the same computation as the original — same columns, same branch structure,
 * same aggregation — written as SQL. Where the original parses a CSV line by index and
 * then branches on the parsed value, the port reads a typed column and branches on it:
 * the parsing is the front end's job here, and the branch is what the tools reason
 * about. Where the original's arithmetic can throw (an integer division, a cast of a
 * malformed string), the port keeps it throwable, because those crashes are what the
 * fuzzing evaluations look for.
 *
 * Sources are pinned in PROVENANCE.md.
 */
object Programs {

  // ---------------------------------------------------------------------------
  // Commute Type — the one program every testing paper here evaluates on.
  // Upstream: cols(3) / cols(4) as an integer speed, then a three-arm branch, then
  // the mean speed per commute type.
  // ---------------------------------------------------------------------------
  object CommuteType extends Benchmark {
    val name = "CommuteType"
    val papers = Seq("BigTest", "BigFuzz", "DepFuzz", "NaturalFuzz")
    val summary = "Mean speed per commute type, from distance and duration"

    val schemas = Map(
      "trips" -> "tid STRING, person STRING, day STRING, distance INT, minutes INT")

    // `DIV` is integer division, as the original's Integer/Integer is, and it throws on
    // a zero duration under ANSI mode — the crash the fuzzing papers report finding.
    val query =
      """SELECT commute, AVG(speed) AS mean_speed
        |FROM (
        |  SELECT CASE WHEN speed > 40 THEN 'car'
        |              WHEN speed > 15 THEN 'public'
        |              ELSE 'onfoot' END AS commute,
        |         speed
        |  FROM (SELECT distance DIV minutes AS speed FROM trips))
        |GROUP BY commute""".stripMargin

    // upstream fault variant: the boundary between `public` and `onfoot` moved
    override val faulty = Some(
      """SELECT commute, AVG(speed) AS mean_speed
        |FROM (
        |  SELECT CASE WHEN speed > 40 THEN 'car'
        |              WHEN speed > 51 THEN 'public'
        |              ELSE 'onfoot' END AS commute,
        |         speed
        |  FROM (SELECT distance DIV minutes AS speed FROM trips))
        |GROUP BY commute""".stripMargin)

    // with the boundary wrong, everything between 15 and 40 mph is called `onfoot`,
    // so that group's mean speed climbs far above walking pace
    override val oracle = Some("commute = 'onfoot' AND mean_speed > 15")

    def rows(count: Int, random: Random): Map[String, Seq[Row]] = {
      val trips = (0 until count).map { i =>
        // a spread that lands in all three arms
        val minutes = 5 + random.nextInt(55)
        val speed = 1 + random.nextInt(70)
        Row(s"t$i", s"p${random.nextInt(count / 4 + 1)}", s"2026-01-${1 + i % 28}",
          speed * minutes, minutes)
      }
      Map("trips" -> trips)
    }
  }

  // ---------------------------------------------------------------------------
  // Commute Type (joined) — the co-dependence case: a trip is only counted if its
  // person appears in the locations table, so mutating one side alone produces nothing.
  // ---------------------------------------------------------------------------
  object CommuteTypeFull extends Benchmark {
    val name = "CommuteTypeFull"
    val papers = Seq("DepFuzz", "NaturalFuzz")
    val summary = "Commute type per city, joining trips to the people who made them"

    val schemas = Map(
      "trips" -> "tid STRING, person STRING, day STRING, distance INT, minutes INT",
      "locations" -> "person STRING, name STRING, state STRING, city STRING")

    val query =
      """SELECT l.city, commute, COUNT(*) AS trips
        |FROM (
        |  SELECT person,
        |         CASE WHEN speed > 40 THEN 'car'
        |              WHEN speed > 15 THEN 'public'
        |              ELSE 'onfoot' END AS commute
        |  FROM (SELECT person, distance DIV minutes AS speed FROM trips)) t
        |JOIN locations l ON t.person = l.person
        |WHERE l.state = 'CA'
        |GROUP BY l.city, commute""".stripMargin

    def rows(count: Int, random: Random): Map[String, Seq[Row]] = {
      val people = math.max(4, count / 10)
      val cities = IndexedSeq("Los Angeles", "San Diego", "Blacksburg", "Fresno")
      val states = IndexedSeq("CA", "CA", "VA", "CA")

      val locations = (0 until people).map { i =>
        val c = i % cities.length
        Row(s"p$i", s"person-$i", states(c), cities(c))
      }
      val trips = (0 until count).map { i =>
        val minutes = 5 + random.nextInt(55)
        val speed = 1 + random.nextInt(70)
        Row(s"t$i", s"p${random.nextInt(people)}", s"2026-01-${1 + i % 28}",
          speed * minutes, minutes)
      }
      Map("trips" -> trips, "locations" -> locations)
    }
  }

  // ---------------------------------------------------------------------------
  // Student Grade — one branch, counted by key.
  // ---------------------------------------------------------------------------
  object StudentGrade extends Benchmark {
    val name = "StudentGrade"
    val papers = Seq("BigTest", "BigFuzz", "DepFuzz")
    val summary = "Pass/fail tally per student"

    val schemas = Map("students" -> "name STRING, score INT")

    val query =
      """SELECT CONCAT(name, CASE WHEN score > 40 THEN ' Pass' ELSE ' Fail' END) AS label,
        |       COUNT(*) AS n
        |FROM students GROUP BY 1""".stripMargin

    // upstream fault variant: the comparison flipped
    override val faulty = Some(
      """SELECT CONCAT(name, CASE WHEN score < 40 THEN ' Pass' ELSE ' Fail' END) AS label,
        |       COUNT(*) AS n
        |FROM students GROUP BY 1""".stripMargin)

    // one corrupt record: a score outside any real grading scale
    override val corrupt = Some(Map("students" -> Row("corrupt", 9999)))

    override val oracle = Some("label LIKE 'corrupt%'")

    def rows(count: Int, random: Random): Map[String, Seq[Row]] = {
      val students = (0 until count).map { i =>
        Row(s"s${i % math.max(1, count / 5)}", random.nextInt(101))
      }
      Map("students" -> students)
    }
  }

  // ---------------------------------------------------------------------------
  // Movie Rating — a filter and a sum by key.
  // ---------------------------------------------------------------------------
  object MovieRating extends Benchmark {
    val name = "MovieRating"
    val papers = Seq("BigTest", "BigFuzz", "DepFuzz")
    val summary = "Total of the high ratings each film received"

    val schemas = Map("ratings" -> "movie STRING, rating INT")

    val query = "SELECT movie, SUM(rating) AS total FROM ratings WHERE rating > 4 GROUP BY movie"

    // upstream fault variant: the threshold off by one, so 4s are counted too
    override val faulty = Some(
      "SELECT movie, SUM(rating) AS total FROM ratings WHERE rating > 3 GROUP BY movie")

    // a rating outside the 1..5 scale inflates one film's total
    override val corrupt = Some(Map("ratings" -> Row("m0", 100000)))

    override val oracle = Some("total > 10000")

    def rows(count: Int, random: Random): Map[String, Seq[Row]] = {
      val ratings = (0 until count).map { i =>
        Row(s"m${i % math.max(1, count / 20)}", 1 + random.nextInt(5))
      }
      Map("ratings" -> ratings)
    }
  }

  // ---------------------------------------------------------------------------
  // Income Aggregation — a filter and a four-arm branch, the widest branch structure
  // in the set.
  // ---------------------------------------------------------------------------
  object IncomeAggregation extends Benchmark {
    val name = "IncomeAggregation"
    val papers = Seq("BigTest", "BigFuzz", "DepFuzz")
    val summary = "Mean income by age band, within one postcode"

    val schemas = Map("income" -> "zipcode STRING, age INT, income INT")

    val query =
      """SELECT band, AVG(income) AS mean_income, COUNT(*) AS n
        |FROM (
        |  SELECT CASE WHEN age >= 40 AND age <= 65 THEN '40-65'
        |              WHEN age >= 20 AND age < 40  THEN '20-39'
        |              WHEN age < 20                THEN '0-19'
        |              ELSE '>65' END AS band,
        |         income
        |  FROM income WHERE zipcode = '90024')
        |GROUP BY band""".stripMargin

    // upstream fault variant: a band boundary that leaves a gap, so 65-year-olds fall
    // through to the `>65` arm
    override val faulty = Some(
      """SELECT band, AVG(income) AS mean_income, COUNT(*) AS n
        |FROM (
        |  SELECT CASE WHEN age >= 40 AND age < 65  THEN '40-65'
        |              WHEN age >= 20 AND age < 40  THEN '20-39'
        |              WHEN age < 20                THEN '0-19'
        |              ELSE '>65' END AS band,
        |         income
        |  FROM income WHERE zipcode = '90024')
        |GROUP BY band""".stripMargin)

    // Every band boundary appears in the data. An age band fault is only visible on the
    // boundary it moves, and a generator that never produces one leaves the fault
    // dormant — the benchmark would then measure nothing while appearing to pass.
    private val boundaries = IndexedSeq(19, 20, 39, 40, 64, 65, 66)

    def rows(count: Int, random: Random): Map[String, Seq[Row]] = {
      val people = (0 until count).map { i =>
        val zip = if (i % 3 == 0) "90024" else f"9${random.nextInt(999)}%04d"
        val age = if (i % 7 == 0) boundaries(random.nextInt(boundaries.length))
                  else random.nextInt(90)
        Row(zip, age, 10000 + random.nextInt(90000))
      }
      Map("income" -> people)
    }
  }

  // ---------------------------------------------------------------------------
  // Find Salary — a branch on the shape of the text, then a cast that can throw.
  // ---------------------------------------------------------------------------
  object FindSalary extends Benchmark {
    val name = "FindSalary"
    val papers = Seq("BigTest", "DepFuzz")
    val summary = "Total of the salaries below a threshold, parsed from text"

    val schemas = Map("salaries" -> "raw STRING")

    // the cast throws on a malformed value under ANSI mode, as the original's toInt does
    val query =
      """SELECT SUM(salary) AS total
        |FROM (
        |  SELECT CASE WHEN substring(raw, 1, 1) = '$' THEN CAST(substring(raw, 2, 5) AS INT)
        |              ELSE CAST(raw AS INT) END AS salary
        |  FROM salaries)
        |WHERE salary < 300""".stripMargin

    def rows(count: Int, random: Random): Map[String, Seq[Row]] = {
      val salaries = (0 until count).map { _ =>
        val value = random.nextInt(500)
        Row(if (random.nextBoolean()) s"$$$value" else value.toString)
      }
      Map("salaries" -> salaries)
    }
  }

  // ---------------------------------------------------------------------------
  // Word Count — no branch at all, which is the point: it is the control.
  // ---------------------------------------------------------------------------
  object WordCount extends Benchmark {
    val name = "WordCount"
    val papers = Seq("BigTest", "BigFuzz", "DepFuzz")
    val summary = "Occurrences of each word"

    val schemas = Map("lines" -> "line STRING")

    val query =
      """SELECT word, COUNT(*) AS n
        |FROM (SELECT explode(split(line, ' ')) AS word FROM lines)
        |WHERE word <> ''
        |GROUP BY word""".stripMargin

    def rows(count: Int, random: Random): Map[String, Seq[Row]] = {
      val vocabulary = IndexedSeq("spark", "lineage", "provenance", "fuzz", "debug", "query")
      val lines = (0 until count).map { _ =>
        Row((0 to random.nextInt(6)).map(_ => vocabulary(random.nextInt(vocabulary.length)))
          .mkString(" "))
      }
      Map("lines" -> lines)
    }
  }

  // ---------------------------------------------------------------------------
  // Weather Analysis — the SoCC subject program, in its SQL form. The RDD form and its
  // oracle live in the BigSift suite; this is here so the testing tools see it too.
  // ---------------------------------------------------------------------------
  object WeatherAnalysis extends Benchmark {
    val name = "WeatherAnalysis"
    val papers = Seq("BigSift", "BigTest")
    val summary = "Snowfall spread per month, over a decade of readings"

    val schemas = Map("weather" -> "zipcode STRING, day STRING, snow DOUBLE")

    val query =
      """SELECT day, MAX(snow) - MIN(snow) AS delta
        |FROM weather GROUP BY day""".stripMargin

    // the SoCC fault: one reading in the wrong unit, which only shows up in the spread
    override val corrupt = Some(Map("weather" -> Row("00001", "01-01", 9999.0)))

    override val oracle = Some("delta > 6000")

    def rows(count: Int, random: Random): Map[String, Seq[Row]] = {
      val readings = (0 until count).map { i =>
        Row(f"${random.nextInt(99999)}%05d", f"${1 + i % 12}%02d-${1 + i % 28}%02d",
          random.nextDouble() * 300)
      }
      Map("weather" -> readings)
    }
  }

  /** Every program, in the order the report lists them. */
  val all: Seq[Benchmark] = Seq(
    CommuteType, CommuteTypeFull, IncomeAggregation, StudentGrade,
    MovieRating, FindSalary, WordCount, WeatherAnalysis)
}
