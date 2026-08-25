# Contributing

BigAsterisk is a migration of thirteen published research systems onto one library that
runs on stock Apache Spark. That shapes what a good contribution looks like here more
than most projects, so this is worth reading before you start.

## The two rules that matter

**1. Nothing that knows about a specific Spark version may live in a tool.**

Every tool here began as a fork of Spark pinned to one release, which is why they all
stopped working. Version-specific code goes behind the `SparkBinding` service interface
in a binding module (`modules/spark4`); tools compile against `modules/api` alone. If a
change makes a tool import `org.apache.spark.sql.catalyst.*`, it belongs in the binding
instead. See [Architecture](docs/architecture.md).

**2. Fail loud, never silently wrong.**

A debugger that returns plausible wrong answers is worse than one that returns none. An
operator outside a verified set, a constraint outside a solver's fragment, a plan the
interpreter cannot take — all of these must be reported, not approximated. There are
tests asserting that unsupported cases *say* they are unsupported.

## Getting set up

```bash
bin/bootstrap            # JDK 17, sbt, Python and Spark into tools/ — nothing global
bin/sbt test             # the Scala suites
python/tests/run.sh      # the PySpark suites
bin/bigasterisk tour     # every tool, end to end
scripts/validate-notebooks.sh
```

Nothing is installed outside the repository, and none of this depends on what is on your
`PATH`.

## What a change should come with

- **A test that fails without it.** For a bug, one that reproduces it; for a behaviour,
  one that pins it. Several of the sharpest bugs in this repository were found by tests
  written before the fix, and the commit messages say so.
- **The claim, checked.** If a change makes something faster, measure it — there is a
  benchmark in `examples/` for that, deliberately separate from the suites, because a
  test that fails when a laptop is busy is a test people learn to ignore.
- **Honest documentation.** Each tool's page in `docs/` states what is implemented *and
  what is not*, and `PROVENANCE.md` records what was and was not reproduced from each
  paper. If your change moves that line, move it in the docs too.

## Adding support for a new Spark release

No tool changes. Add `modules/sparkN` implementing `SparkBinding`, declare the releases
it handles, and register it in
`META-INF/services/org.bigasterisk.api.SparkBinding`. `Spark4BindingSuite` is the
template for its test: it drives a tool end to end through `org.bigasterisk.api` only,
with no engine types in sight, which is the property that keeps tools portable.

## Style

- **Match the surrounding code.** Scala here is wrapped at 100 columns with JavaDoc-style
  docstrings. There is deliberately no automatic formatter: scalafmt was tried and
  rejected, because no configuration reproduced the existing style closely enough to
  avoid a three-thousand-line cosmetic diff across the inherited engine code, and losing
  that much `git blame` buys nothing a reviewer cannot see.
- Comments should explain *why*, not restate the code. Most of the comments here exist
  because something was surprising — a Catalyst behaviour, a paper's actual design, an
  attribution that landed on the wrong record — and saying which is the point.
- Each tool's documentation explains that tool. Cross-referencing another tool inside an
  explanation makes both harder to learn.

## Papers

Every tool corresponds to a published paper, listed in [docs/citations.md](docs/citations.md).
When changing a tool's behaviour, check the paper: several gaps in this codebase closed
once the paper turned out to describe something more tractable than assumed.
