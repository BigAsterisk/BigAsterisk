# Running on a cluster

`local[*]` runs every executor inside the driver JVM. That is convenient, and it hides
the failures that matter: a closure that only the driver's classloader can resolve,
capture state that never crosses a process boundary, a block that is never fetched over
the network. None of those can happen when there is only one JVM.

So the platform is exercised two ways beyond in-process: a standalone master and worker
started from the Spark distribution, and a Compose stack where every role is its own
container.

!!! note "This is not a formality"

    Moving the platform tour onto a real standalone cluster immediately failed three
    tools. The block-manager filter that finds lineage blocks was being shipped to the
    executors, where Spark's own RPC deserializes it — and a lambda compiled into a jar
    attached with `--jars` lives in a classloader that path cannot see. The fix was to
    filter on the driver, which it can, because every lineage block is registered with
    the master when it is stored. A `local-cluster` test never caught it: those executors
    share the test classpath.

## Without Docker

If you have run `bin/bootstrap`, the Spark distribution is already in `tools/`:

```bash
bin/sbt package
scripts/standalone-tour.sh
```

That starts a master and a worker as separate processes, submits the platform tour with
`--jars`, and tears the cluster down afterwards. It is the same script CI runs on every
push, so cluster-mode breakage fails the build rather than being discovered later.

## With Docker Compose

```bash
scripts/cluster.sh up 3        # master + 3 workers + history server
scripts/cluster.sh tour        # every tool, against the cluster
scripts/cluster.sh status      # what the master actually sees
scripts/cluster.sh down
```

The first `up` builds the image, which compiles the platform from source — several
minutes, then cached.

| Service | Role | UI |
|---|---|---|
| `master` | standalone master, submit endpoint on 7077 | <http://localhost:8080> |
| `worker` | one container each, `--scale worker=N` | 8081, mapped to a free port |
| `history` | completed applications, stage timings, executor metrics | <http://localhost:18080> |
| `submit` | client; runs a job and exits | — |

Every role runs the **same image**, so master, workers and client cannot disagree about
the Spark build, the JDK or the platform jars — a mismatch there produces failures that
look like bugs in the tools.

### Running something other than the tour

```bash
scripts/cluster.sh run bigsift weather max          # a BigSift scenario
scripts/cluster.sh run benchmark capture            # capture overhead, on the cluster
scripts/cluster.sh run pyspark                      # a shell attached to the cluster
scripts/cluster.sh run submit org.example.MyJob     # any main class in the image
```

Or drive Compose directly:

```bash
docker compose -f docker/compose.yaml up -d --scale worker=3
docker compose -f docker/compose.yaml run --rm submit tour
```

### Submitting from the host

Port 7077 is published, so a checkout can submit into the containerised cluster:

```bash
tools/spark-4.1.2-bin-hadoop3/bin/spark-submit \
  --master spark://localhost:7077 \
  --class org.bigasterisk.examples.PlatformTour \
  --jars "$(ls modules/*/target/scala-2.13/*.jar | paste -sd, -)" \
  examples/target/scala-2.13/bigasterisk-examples_2.13-0.1.0-SNAPSHOT.jar
```

The one thing to get right is that **inputs must resolve to the same path everywhere**.
The Compose file mounts `examples/data` read-only into every container for exactly that
reason; a path that exists only on the driver produces a `PATH_NOT_FOUND` from the
executors.

## Attaching the platform to your own cluster

Nothing above is special to the demo. On any Spark 4.1.x cluster:

```bash
spark-submit \
  --master spark://your-master:7077 \
  --jars bigasterisk-api.jar,bigasterisk-spark4.jar,bigasterisk-bigsift.jar,bigasterisk-optdebug.jar,fastutil.jar \
  --conf spark.sql.extensions=org.apache.spark.sql.lineage.TitianSQLExtension \
  your-application.jar
```

Add the extension for the tool you want; each tool's page lists its own. Spark itself is
never modified and never restarted with a patched build — the jars attach to the cluster
you already run.

## Limitations

- **The Compose stack is not built in CI.** An image that compiles the platform from
  source is too slow for every push, so CI validates the Compose definition and runs the
  process-level standalone cluster instead. The image itself is verified by running it.
- **Standalone only.** No YARN or Kubernetes manifests yet. The `--jars` attachment above
  is scheduler-agnostic, but nothing here proves that on those schedulers.
- **One machine.** Compose puts every container on one host, so it exercises process and
  classloader boundaries and real network fetches between JVMs — not multi-host latency,
  data locality or straggler behaviour.
