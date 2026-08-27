# A guided demo: every tool, on a real cluster

Thirteen tools, one planted fault, executors running in their own containers. Every
command below was run to produce the output shown next to it — nothing here is an
example of what you might see.

**Time:** about 15 minutes, most of it the first image build.
**Needs:** Docker, and this repository. Nothing else — no Spark, no JDK, no Python.

---

## Step 0 — Start the cluster

```bash
git clone https://github.com/BigAsterisk/BigAsterisk.git
cd BigAsterisk
scripts/cluster.sh up 3
```

The first run builds the image: it compiles the platform from source and downloads Spark,
so allow several minutes. Afterwards it is cached and `up` takes seconds.

```
master UI:  http://localhost:8080   (expect 3 workers registered)
history:    http://localhost:18080
```

Check the master agrees:

```bash
scripts/cluster.sh status
```

```
NAME               STATUS                    PORTS
docker-master-1    Up 26 seconds (healthy)   0.0.0.0:7077->7077/tcp, 0.0.0.0:8080->8080/tcp
docker-worker-1    Up 20 seconds             127.0.0.1:55000->8081/tcp
docker-worker-2    Up 20 seconds             127.0.0.1:55002->8081/tcp
docker-worker-3    Up 20 seconds             127.0.0.1:55001->8081/tcp
docker-history-1   Up 20 seconds             0.0.0.0:18080->18080/tcp

Cores in use: 6 Total
```

Three workers, two cores each. Open <http://localhost:8080> and you will see them
registered — these are separate JVMs in separate containers, which is the whole point:
`local[*]` would run every executor inside the driver and hide the failures that only
appear when they are elsewhere.

## Step 1 — Everything at once

```bash
scripts/cluster.sh tour
```

Ends with:

```
TOUR OK — every tool ran
```

That is the smoke test. The rest of this page runs the tools one at a time so you can
read what each actually says.

---

## The fault everything is chasing

One query over twelve orders. Amounts above 1000 are negated — a sign error that only
the outlier record `o8` (99999) ever hits:

```sql
SELECT cid, SUM(CASE WHEN amount > 1000 THEN -amount ELSE amount END) AS total
FROM orders GROUP BY cid
```

Customer `c2`'s total comes out as **-99354** instead of 100644.

---

## Step 2 — DeSQL: what the query is made of

```bash
scripts/cluster.sh tour desql
```

```
── DeSQL — step through the query
  [0] Relation     orders
  [1] Aggregate    orders.cid, sum(CASE WHEN (orders.amount > 1000) THEN (- orders.amount)
                   ELSE orders.amount END) AS total GROUP BY orders.cid
```

The query decomposed into steps you can materialise individually. This is where the other
tools get their notion of "an operation".

## Step 3 — Titian: which records produced the wrong result

```bash
scripts/cluster.sh tour titian
```

```
── Titian — which records produced the wrong total
  faulty output: [c2,-99354]
  provenance returns 4 records: [c2,250], [c2,190], [c2,99999], [c2,205]
```

Record-level provenance, captured across executors and traced back to the source rows.
Four candidates — correct, and not yet an answer. Every record in the group contributed.

## Step 4 — FlowDebug: which of them *mattered*

```bash
scripts/cluster.sh tour flowdebug
```

```
── FlowDebug — which of them actually mattered
  0.9936  [o8,c2,99999]  (contribution 99.4% of the total magnitude)  via amount
  0.0025  [o2,c2,250]    (contribution 0.2% of the total magnitude)   via amount
  0.0020  [o11,c2,205]   (contribution 0.2% of the total magnitude)   via amount
```

Four candidates become one that carries 99.4% of the responsibility. `via amount` is the
taint refinement: of the record's three columns, only `amount` could reach the result.

## Step 5 — BigSift: the minimum input that reproduces it

```bash
scripts/cluster.sh tour bigsift
```

```
── BigSift — which input records are to blame (data-space)
  provenance left 4 candidate records; delta debugging narrowed them to 1
```

Delta debugging over the provenance, re-running the query on subsets until it has the
smallest input that still fails. Four to one.

## Step 6 — OptDebug: which *operation* is at fault

```bash
scripts/cluster.sh tour optdebug
```

```
── OptDebug — which operation is to blame (code-space)
  1.000  [1] Aggregate branch — (orders.amount > 1000)   (failing=1, passing=0)
  0.500  [1] Aggregate — ... GROUP BY orders.cid          (failing=1, passing=8)
```

The other four steps found the bad *data*. This finds the bad *code*: the branch
`amount > 1000` scores 1.0 because every failing record takes it and no passing record
does. That branch is the sign error.

## Step 7 — BigDebug: the interactive primitives

```bash
scripts/cluster.sh tour bigdebug
```

```
── BigDebug — a breakpoint: the state at a point, without pausing
  4 records were flowing past; the first few: ...

── BigDebug — a watchpoint on the records flowing past
  1 record(s) matched `>`(amount, 1000)
    [o8,c2,99999]

── BigDebug — which record killed the query
  partition 0, record 7: [o8,c2,99999]
  SparkArithmeticException: [DIVIDE_BY_ZERO] ...
```

Three primitives: a breakpoint that regenerates the state at a point without pausing the
job, a watchpoint that captures matching records as they flow, and a crash guard that
names the exact record that killed a task — partition and index — rather than a stack
trace.

## Step 8 — PerfDebug: which record cost too much

```bash
scripts/cluster.sh tour perfdebug
```

```
── PerfDebug — which record cost too much
  11 records, skew 2.0x the mean
```

Per-record latency attribution. Twelve rows is far too small for the skew number to mean
much — see the note under [Benchmarks](benchmarks.md) — but it demonstrates the
attribution working across a real cluster.

## Step 9 — Vega: what the next revision can reuse

```bash
scripts/cluster.sh tour vega
```

```
── Vega — the next revision reuses what it can
  reused 1 of 3 parts (33%): Filter — (orders.amount > 100)
```

You edit a query and run it again. Vega reuses the intermediate results the edit did not
invalidate instead of recomputing from scratch.

## Step 10 — The three fuzzers, side by side

This is the clearest comparison in the demo. One joined query, three mutation strategies —
the three papers differ in exactly one decision, where a generated value comes from.

```bash
scripts/cluster.sh tour bigfuzz
scripts/cluster.sh tour depfuzz
scripts/cluster.sh tour naturalfuzz
```

```
── BigFuzz — values drawn for the column's type
  coverage 50% of 2 branches, 19 empty
  19 of 20 iterations produced nothing: a randomly generated join key rarely matches

── DepFuzz — join equalities repaired across tables
  coverage 100% of 2 branches, 0 empty
  0 of 20 iterations produced nothing: the two sides of the join are kept together

── NaturalFuzz — rows spliced out of real ones
  coverage 100% of 2 branches, 0 empty
  branches reached: (NOT (o.amount > 100)), (o.amount > 100)
```

**19 of 20 against 0 of 20.** That is DepFuzz's claim, live: generate a join key from
nothing and essentially no row survives the join, so the campaign learns nothing from 19
of its 20 iterations. Repair the equality and every iteration produces output.

## Step 11 — BigTest and NaturalSym: an input per path

```bash
scripts/cluster.sh tour bigtest
scripts/cluster.sh tour naturalsym
```

```
── BigTest — an input per path through the query
  [ok] NOT (orders.amount > 100)  (verified)
    orders: [o1,c2,80]
  [ok] (orders.amount > 100)  (verified)
    orders: [o12,c3,310]

── NaturalSym — the same paths, with values that look real
  [ok] NOT (orders.amount > 100)  (verified)
  [ok] (orders.amount > 100)  (verified)
  coverage 100% of 1 branches
```

Fuzzing searches for inputs; this constructs them, one per path through the query's
conditions. `(verified)` means the generated input was executed and really did take the
path it was built for — a generator that reports coverage it did not achieve is worse
than useless.

NaturalSym reaches the same paths with witnesses drawn from values that actually occur,
shaped by a declared distribution, so the generated records read like records.

---

## Step 12 — The PySpark front end, and reading inside a UDF

Everything above went through Scala. The same tools are available from Python:

```bash
scripts/cluster.sh run pydemo
```

```
== connected to spark://master:7077
   executors: 3

== Titian — the records behind a wrong result
   wrong output: Row(cid='c2', total=-99354)
   4 source records produced it

== FlowDebug — which of those records mattered
   0.9936  {'oid': 'o8', 'cid': 'c2', 'amount': 99999}  ...  via amount

== Fuzzing — three strategies on one joined query
   random        coverage 50%   empty results 19/20
   natural       coverage 100%  empty results 0/20
   co-dependent  coverage 100%  empty results 0/20

== Reading inside a Python UDF
   UdfProfile(classify(amount), 2 branches, 3 paths, solvable)
   branch: amount > 1000
   branch: amount > 100
   3 of 4 generated inputs verified
   [ok] NOT (orders.amount > 1000) AND (orders.amount > 100)  (verified)
    orders: [o5,c2,205], [o2,c1,310], [o11,c3,420]
```

The last section is the UDF boundary being crossed. `classify` is an ordinary Python
function; `bigasterisk.udf.register` parses its source, and from then on
`WHERE classify(amount) = 'high'` is solvable — the condition on the function's *result*
becomes conditions on its *argument*. Without the profile, no input can be generated for
it at all.

## Step 13 — Reproduce a paper's overhead number

```bash
scripts/cluster.sh run benchmark capture
```

```
=== Titian capture-overhead benchmark (2000000 rows, median of 7) ===
RDD reduceByKey              off:  206 ms   on:  261 ms   overhead: 26.7%
SQL groupBy aggregate        off:  457 ms   on:  614 ms   overhead: 34.4%
SQL join + aggregate         off:  694 ms   on: 1099 ms   overhead: 58.4%
SQL join backward trace (1 row): 448 ms   lineage blocks: 22.9 MB mem
```

Two million rows on the cluster. The RDD figure — **26.7%** — is the one Titian's paper
makes a claim about (under 30%), measured here rather than quoted.

The framework-abstraction benchmark is worth running too:

```bash
scripts/cluster.sh run benchmark fuzz
```

```
50 iterations of the same campaign:
  through Spark   4288 ms  ( 85.8 ms per iteration)
  abstracted        85 ms  (  1.7 ms per iteration)
  speedup         50.4x
```

## Step 14 — Look at what ran

<http://localhost:18080> — the history server has every application, with stage timings
and executor metrics. Useful for convincing yourself the work really happened out on the
workers.

## Step 15 — Tear down

```bash
scripts/cluster.sh down
```

Removes the containers, the network and the event-log volume. The image stays cached for
next time.

---

## Doing more

| Command | What it does |
|---|---|
| `scripts/cluster.sh tour <tool>...` | any subset: `tour titian bigsift optdebug` |
| `scripts/cluster.sh run bigsift weather max` | a larger BigSift scenario on generated data |
| `scripts/cluster.sh run pyspark` | an interactive PySpark shell attached to the cluster |
| `scripts/cluster.sh run submit <class>` | any main class in the image |
| `scripts/cluster.sh up 6` | more workers |
| `scripts/cluster.sh logs worker` | follow the workers |

Tool names for `tour`: `desql`, `titian`, `flowdebug`, `bigsift`, `optdebug`, `bigdebug`,
`perfdebug`, `vega`, `bigfuzz`, `depfuzz`, `naturalfuzz`, `bigtest`, `naturalsym`.

## Without Docker

Everything above also runs against a standalone cluster started from a local Spark
distribution — see [Running on a cluster](cluster.md):

```bash
bin/bootstrap && bin/sbt package
scripts/standalone-tour.sh
```

That is the form CI runs on every push.
