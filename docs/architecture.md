# Architecture

BigAsterisk has one structural rule: **nothing that knows about a specific Spark
version may live in a tool.**

Every tool in this repository started life as a fork of Apache Spark. That is the
reason they stopped working — a fork is pinned to the release it was cut from, and
each new Spark release strands it further. The platform's job is to make sure that
never happens again.

## The binding layer

```
                    ┌──────────────────────────────────────────┐
   your job  ───►   │  modules/api                             │
                    │    BigAsterisk        entry point        │
                    │    LineageSupport     what tools call    │
                    │    SparkBinding       the SPI            │
                    └───────────────────┬──────────────────────┘
                                        │  ServiceLoader, at runtime
                       ┌────────────────┴────────────────┐
                       ▼                                 ▼
             ┌───────────────────┐             ┌───────────────────┐
             │ modules/spark4    │             │ modules/spark5    │
             │ "[4.0.0,5.0.0)"   │             │ "[5.0.0,6.0.0)"   │
             │                   │             │    (future)       │
             │ codegen taps      │             │                   │
             │ RDD taps          │             │                   │
             │ block storage     │             │                   │
             └───────────────────┘             └───────────────────┘
                       │                                 │
                       ▼                                 ▼
              stock Spark 4.1.x                  stock Spark 5.x
```

`modules/api` contains no Spark internals. It depends on `spark-sql` only for the
public types that appear in signatures — `SparkSession`, `DataFrame`, `Row` — and those
are stable across releases.

Everything that is version-specific — physical-plan rules, whole-stage-codegen tap
operators, executor-side block storage, the `private[spark]` internals layer — is
confined to a binding module.

## How a binding is selected

1. `BigAsterisk.bindings` enumerates every `SparkBinding` on the classpath through
   `java.util.ServiceLoader`, reading
   `META-INF/services/org.bigasterisk.api.SparkBinding`.
2. `BigAsterisk.select` keeps those whose declared `sparkVersions` range contains the
   running Spark version, and picks the most specific one — the highest lower bound —
   so a binding published for one release wins over a broader fallback.
3. `BigAsterisk.binding(spark)` then calls `validate`, which fails with actionable
   guidance if the session was built without the binding's `spark.sql.extensions`.

Version ranges are Maven-style (`"[4.0.0,5.0.0)"`). A qualifier is ignored when
comparing, so Spark `4.1.0-preview1` resolves to the binding for the `4.1.0` line
rather than falling off the end of the range.

Discovery is repeated on each call rather than cached, so a binding added to a live
classpath — a notebook `%AddJar`, a REPL `:require` — is picked up without a restart.

## Adding support for a new Spark release

No tool module changes. Create the binding, declare its range, register it:

1. `modules/sparkN/src/main/scala/org/bigasterisk/sparkN/SparkNBinding.scala`
   implementing [`SparkBinding`](https://github.com/BigAsterisk/BigAsterisk/blob/main/modules/api/src/main/scala/org/bigasterisk/api/SparkBinding.scala):

   ```scala
   class SparkNBinding extends SparkBinding {
     def name = "sparkN"
     def sparkVersions = "[5.0.0,6.0.0)"
     def requiredConf = Map("spark.sql.extensions" -> classOf[MyExtension].getName)
     def validate(spark: SparkSession): Unit = ...
     val lineage: LineageSupport = new SparkNLineage
   }
   ```

2. Register it:

   ```
   modules/sparkN/src/main/resources/META-INF/services/org.bigasterisk.api.SparkBinding
   ```
   containing `org.bigasterisk.sparkN.SparkNBinding`.

3. Add the module to `build.sbt` and aggregate it in `root`.

`Spark4BindingSuite` is the template for the binding's own test: it drives provenance
end to end through `org.bigasterisk.api` only, with no engine types in sight, which is
exactly the property that keeps tools portable.

## Why `org.apache.spark` packages still appear

The capture engine in `modules/spark4` declares classes under `org.apache.spark.*`.
This is deliberate and confined to the binding: tap operators must subclass Spark's
`SparkPlan` and `RDD` and reach a small number of `private[spark]` members to attach at
stage boundaries. Doing this from a binding module — rather than from a patched Spark
build — is what keeps the distribution stock. The surface used is enumerated in the
[developer guide](developer-guide.md).

## Module map

| Module | Depends on | Contains |
|---|---|---|
| `modules/api` | `spark-sql` (provided) | Tool API, `SparkBinding` SPI, version matching |
| `modules/spark4` | `api` | Spark 4.x capture engine and binding |
| `modules/bigsift` | `api`, `spark4` | Delta-debugging isolation over the lineage API |
| `examples` | all | Runnable demos and benchmarks |

Spark itself is a `Provided` dependency everywhere: BigAsterisk attaches to the Spark
you already run, and never ships one.
