# DeSQL — step-through debugging for SQL

A SQL query is normally all-or-nothing: you write it, you run it, and you get an
answer. When the answer is wrong there is nothing to inspect in between. DeSQL breaks
a query into its constituent parts and gives you the intermediate data at each one —
the SQL equivalent of stepping through a program and watching a variable.

Introduced in *DeSQL: Interactive Debugging of SQL in Data-Intensive Scalable
Computing* (FSE 2024).

## Using it

```scala
import org.bigasterisk.api.BigAsterisk

val df = spark.sql(
  """SELECT c.name, SUM(o.amount) AS total
    |FROM orders o JOIN customers c ON o.cid = c.cid
    |WHERE o.amount > 100
    |GROUP BY c.name""".stripMargin)

BigAsterisk.desql(spark).decompose(df).foreach { step =>
  println(s"[${step.id}] ${step.operator} — ${step.detail}")
  step.data.show()
}
```

```python
import bigasterisk

for step in bigasterisk.desql(spark).decompose(query):   # DataFrame or SQL text
    print(step)
    step.data.show()
```

Output for the query above:

```
[0] Relation — orders AS o
[1] Relation — customers AS c
[2] Join — INNER ON (o.cid = c.cid)
[3] Filter — (o.amount > 100)
[4] Aggregate — c.name, sum(o.amount) AS total GROUP BY c.name
```

The steps follow the **analyzed** plan, so they are in the order the query says, not
the order Spark will eventually execute: the `WHERE` sits above the join here, even
though the optimizer will push it below.

Steps are ordered so that every step appears after the steps feeding it, and
`childIds` says which ones those are. `step.data` is an ordinary `DataFrame`, so you
can count it, filter it, or join it against expected results.

## What a step is

Each step is one node of the query's **analyzed logical plan**. Attributes in a
Catalyst plan flow strictly bottom-up, so the subtree rooted at any node is itself a
complete, resolved query computing "the query so far". Materialising `step.data` runs
exactly that subtree — the work up to that point, and no more.

Nodes that do not change the data are folded away, so the steps line up with parts you
would recognise in your own query rather than with Catalyst bookkeeping. `FROM orders o`
on a temp view nests three such wrappers — `SubqueryAlias(o)`, `View(orders)`,
`SubqueryAlias(orders)` — above the scan; all three collapse, and their names are
carried down so the scan reports itself as `orders AS o`. A plan reused in two branches
becomes a single step.

`step.schema` is available without materialising anything, so you can inspect the shape
of an intermediate result without paying to compute it.

## Seeing one step in full

`decompose` gives you the shape of the query; each step then answers three different
questions about itself.

```python
steps = bigasterisk.desql(spark).decompose(query)

for step in steps:
    print("[%d] %s  %s" % (step.id, step.operator, step.detail))
```

```text
[0] Relation     orders AS o
[1] Relation     customers AS c
[2] Join         INNER ON (o.cid = c.cid)
[3] Filter       (o.amount > 100)
[4] Aggregate    c.name, count(1) AS n GROUP BY c.name
```

`detail` is the step's **own** operator — a join condition, a grouping list. To see the
whole sub-query at that point, ask for its plan:

```python
print(steps[3].plan)
```

```text
Filter (amount#2 > 100)
+- Join Inner, (cid#1 = cid#10)
   :- SubqueryAlias o
   :  +- Relation [oid#0,cid#1,amount#2] csv
   +- SubqueryAlias c
      +- Relation [cid#10,name#11] csv
```

And to see the **data** flowing through it — the point of the whole exercise — every step
is a DataFrame:

```python
steps[3].data.show()          # the rows at that point
steps[3].data.count()         # how many there are
steps[3].schema               # what they look like
steps[3].data.filter("amount > 300").show()    # it is an ordinary DataFrame
```

```text
+---+---+------+---+-----+
|oid|cid|amount|cid| name|
+---+---+------+---+-----+
| o1| c1|   420| c1|Alice|
| o2| c2|   250| c2|  Bob|
| o4| c1|   310| c1|Alice|
+---+---+------+---+-----+
```

Materialising a step runs the query **up to that point and no further**, so working
backwards from a wrong answer costs only the part of the pipeline you are still
suspicious of.

Each step's branches carry their own data too — the records that took one arm of a
condition:

```python
for branch in steps[3].branches:
    print(branch.description, branch.data.count())
```

```text
(o.amount > 100)        8
(NOT (o.amount > 100))  4
```

From the command line, `--step` shows all of this for one step at once:

```bash
bin/bigasterisk analyze \
  --table orders=examples/data/orders.txt \
  --schema orders="oid STRING, cid STRING, amount INT" \
  --query "SELECT cid, SUM(amount) AS total FROM orders GROUP BY cid" \
  --tool desql --step 1
```

## Limitations

- **Correlated subqueries.** A subquery that references the enclosing query carries
  outer references and is not independently executable. Those steps raise a clear
  `UnsupportedOperationException` rather than returning misleading rows; inspect the
  enclosing step instead.
- **Spark Connect.** Decomposition runs against the driver-side analyzed plan, which a
  Connect client does not hold. Classic sessions only.
- **Cost.** Materialising every step re-runs the query prefix for each one. On a large
  input, decompose first, then materialise only the steps you care about — `decompose`
  itself does not run anything.

## Relationship to the published tool

This is a **reimplementation**, not a port. The original obtained the same
decomposition by adding a `mappingIndex` field and visitor hooks to Catalyst's own
plan and expression classes, which required distributing a forked Spark. Re-deriving
the decomposition from the unmodified analyzed plan produces the same steps with no
fork, which is what lets it run on a stock Spark installation.

See [PROVENANCE.md](https://github.com/BigAsterisk/BigAsterisk/blob/main/PROVENANCE.md)
for the upstream source this was checked against.
