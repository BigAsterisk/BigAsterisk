# The published subject programs

Every claim these tools make is a claim about a particular set of programs. A platform
that runs only on its own fixtures cannot say whether it holds up on any of them, so the
subject programs from the published evaluations are ported here and every tool is run
over them.

```bash
bin/sbt 'benchmarks/runMain org.bigasterisk.benchmarks.BenchmarkRunner'
bin/sbt 'benchmarks/runMain org.bigasterisk.benchmarks.BenchmarkRunner CommuteType'
```

It writes `benchmarks-results.md` and `benchmarks-results.csv`, and exits non-zero if any
measurement errored.

!!! warning "These are measurements, not reproductions"

    Each number is what the tool found on that program. **None is a ratio against a
    baseline**, because no baseline is implemented — reproducing "10³–10⁷× more precise
    than provenance" or "2× the faults found" needs plain provenance and an unguided
    generator to compare against, and neither exists here. Read the table as *the tools
    work on the workloads the claims were made about*, not as *the claims are
    reproduced*.

## The programs

Ported from the upstream artifacts pinned in
[PROVENANCE.md](https://github.com/BigAsterisk/BigAsterisk/blob/main/PROVENANCE.md) —
same columns, same branch structure, same aggregation, written as SQL because that is the
surface the tools attach to here.

| Program | Papers | What it computes |
|---|---|---|
| CommuteType | BigTest, BigFuzz, DepFuzz, NaturalFuzz | Mean speed per commute type, from distance and duration |
| CommuteTypeFull | DepFuzz, NaturalFuzz | The same, joined to the people who made the trips |
| IncomeAggregation | BigTest, BigFuzz, DepFuzz | Mean income by age band, within one postcode |
| StudentGrade | BigTest, BigFuzz, DepFuzz | Pass/fail tally per student |
| MovieRating | BigTest, BigFuzz, DepFuzz | Total of the high ratings each film received |
| FindSalary | BigTest, DepFuzz | Total of the salaries below a threshold, parsed from text |
| WordCount | BigTest, BigFuzz, DepFuzz | Occurrences of each word — the control, with no branch |
| WeatherAnalysis | BigSift, BigTest | Snowfall spread per month |

`CommuteType` is the only program every testing paper here evaluates on, which makes it
the one worth reading first.

## Two fault models, because the papers use two

- A **mutated program** — a moved boundary, a flipped comparison, an off-by-one
  threshold, in the style of BigTest's `BenchmarksFault` variants. This is what a
  technique that localises *operations* has to find.
- A **corrupt record** — one bad row in otherwise clean data, as the SoCC subject
  programs plant. This is what a technique that isolates *inputs* has to find.

A benchmark carries whichever its upstream evaluation used, and a tool whose fault model
a program does not provide is reported as `—` rather than being given an invented one.

Wrongness under the mutated-program model is decided **differentially**: a row is wrong
when the correct program does not produce it. A threshold would need recalibrating for
every program and every input size, which is how an oracle silently stops testing
anything.

## Results

At 400 rows per table, seed 1:

| Program | DeSQL | BigTest | Fuzzing | Titian | BigSift | OptDebug | FlowDebug |
|---|---|---|---|---|---|---|---|
| CommuteType | 4 steps | 0% | 100% | 160 witnesses | — | 0.49 | 0.01 |
| CommuteTypeFull | 7 steps | 0% | 100% | 25 witnesses | — | — | — |
| IncomeAggregation | 4 steps | 0% | 100% | 35 witnesses | — | 1.00 | — |
| StudentGrade | 2 steps | 100% | 100% | 1 witness | **1 of 401** | 0.49 | 1.00 |
| MovieRating | 3 steps | 100% | 100% | 3 witnesses | **1 of 401** | 1.00 | 1.00 |
| FindSalary | 4 steps | 0% | 100% | 249 witnesses | — | — | — |
| WordCount | 5 steps | 100% | 100% | 191 witnesses | — | — | — |
| WeatherAnalysis | 2 steps | 100% | 100% | 5 witnesses | **2 of 401** | — | 1.00 |

43 measurements, 13 not applicable, 0 errors. What each column means: branch coverage for
BigTest and Fuzzing, witnesses behind one output row for Titian, the isolated
fault-inducing input set for BigSift, the top suspiciousness score for OptDebug, the top
influence score for FlowDebug.

Two results are worth reading closely.

**BigSift narrows 401 records to one.** On the three programs with a planted corrupt
record, delta debugging over provenance returns the planted row and nothing else — two
rows on WeatherAnalysis, where the spread needs a pair. This is the precision the SoCC
paper reports, measured here rather than quoted.

**BigTest covers nothing on four programs, and that is a real gap.** `CommuteType`,
`CommuteTypeFull`, `IncomeAggregation` and `FindSalary` all branch on a *computed* value
— `distance DIV minutes > 40`, `substring(raw, 1, 1) = '$'`. The solver here is an
interval-and-equality domain per column, which cannot invert arithmetic or string
functions, so it reports those paths unsupported instead of generating an input that
would not take them. The original BigTest drives cvc5 and would solve them. This is the
clearest measured consequence of not having an SMT solver, and it is why the number is
printed rather than the program quietly skipped.

Fuzzing reaches 100% on all eight because it does not need to invert anything — it runs
the query and observes. It also finds the crash the fuzzing papers report: one
divide-by-zero in `CommuteType`, and twelve malformed-number failures in `FindSalary`.

## What is still missing

- **No baselines.** The largest gap. Every ratio in the papers needs one.
- **Roughly half the programs.** The upstream artifacts also ship AirportTransit (covered
  separately by the BigSift verification suite), the PigMix queries L2–L5, InsideCircle,
  MapString, NumberSeries, ExternalCall, LoanType, AgeAnalysis, Delays, FlightDistance,
  DeliveryFaults and the NaturalFuzz case studies. Adding one is a `Benchmark` object and
  a generator.
- **Small inputs.** 400 rows per table measures behaviour, not throughput. Performance
  claims need the TPC-DS harness and a cluster; see
  [Ablation & performance](ablation.md) and [Running on a cluster](cluster.md).
- **PerfDebug, Vega and BigDebug are not in the sweep.** They need a workload shape these
  programs do not have — computation skew, a query revision history, an interactive
  session.

## Adding a program

```scala
object MyProgram extends Benchmark {
  val name = "MyProgram"
  val papers = Seq("BigFuzz")
  val summary = "..."
  val schemas = Map("input" -> "a STRING, b INT")
  val query = "SELECT ..."
  override val faulty = Some("SELECT ...")     // a mutated variant, if the paper has one
  override val corrupt = Some(Map("input" -> Row("bad", 9999)))
  override val oracle = Some("...")            // SQL over the output columns

  def rows(count: Int, random: Random): Map[String, Seq[Row]] = ...
}
```

Add it to `Programs.all`. The suite checks that the data is deterministic in its seed,
that an injected fault really changes the answer, and that a planted record really trips
the oracle — a benchmark whose fault is dormant measures nothing while appearing to pass.
