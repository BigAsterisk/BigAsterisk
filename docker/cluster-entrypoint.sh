#!/bin/sh
# Role dispatcher for the BigAsterisk cluster image. Every role runs from the same
# image so that master, workers and the client cannot drift apart in Spark version,
# JDK or platform jars — a mismatch there produces failures that look like bugs in the
# tools rather than what they are.
set -e

MASTER_URL="${SPARK_MASTER_URL:-spark://master:7077}"

# py4j is found rather than named: its version travels with the Spark distribution, and
# pinning it in the image would break on the next Spark bump for no reason.
PY4J="$(ls "$SPARK_HOME"/python/lib/py4j-*-src.zip 2>/dev/null | head -1)"
[ -n "$PY4J" ] && PYTHONPATH="$PYTHONPATH:$PY4J"
export PYTHONPATH
EVENT_LOG_DIR="${SPARK_EVENT_LOG_DIR:-/opt/bigasterisk/events}"

# Every jar the platform ships, as one comma-separated --jars value. Shipping them with
# --jars rather than baking them onto the executor classpath is deliberate: it is how a
# user would actually attach BigAsterisk, so the demo exercises the same class loading
# path they will.
jars() {
  find /opt/bigasterisk/jars -name '*.jar' | sort | paste -sd, -
}

# The examples jar carries the demo main classes; the rest are the platform.
main_jar() {
  find /opt/bigasterisk/jars -name 'bigasterisk-examples*.jar' | head -1
}

submit() {
  class="$1"
  shift
  mkdir -p "$EVENT_LOG_DIR"
  exec "$SPARK_HOME/bin/spark-submit" \
    --master "$MASTER_URL" \
    --class "$class" \
    --jars "$(jars)" \
    --conf spark.eventLog.enabled=true \
    --conf "spark.eventLog.dir=$EVENT_LOG_DIR" \
    --conf "spark.executor.cores=${EXECUTOR_CORES:-1}" \
    --conf "spark.executor.memory=${EXECUTOR_MEMORY:-1g}" \
    --conf "spark.cores.max=${CORES_MAX:-4}" \
    "$(main_jar)" "$@"
}

case "${1:-help}" in
  master)
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host "${SPARK_MASTER_HOST:-master}" --port 7077 --webui-port 8080
    ;;

  worker)
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      "$MASTER_URL" \
      --cores "${SPARK_WORKER_CORES:-2}" --memory "${SPARK_WORKER_MEMORY:-2g}" \
      --webui-port 8081
    ;;

  history)
    mkdir -p "$EVENT_LOG_DIR"
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.history.HistoryServer
    ;;

  # ---- client roles ---------------------------------------------------------

  tour)
    # Every tool, on one planted fault, against the real cluster. Exits non-zero if any
    # section fails, so `docker compose run submit tour` is a usable smoke test.
    # Naming tools runs only those: `tour titian bigsift`.
    shift
    submit org.bigasterisk.examples.PlatformTour "$@"
    ;;

  analyze)
    # The general entry point: your query, your data, any tool. Mount your data in and
    # name it — nothing here is specific to the bundled examples.
    shift
    submit org.bigasterisk.examples.Analyze "$@"
    ;;

  bigsift)
    shift
    submit org.bigasterisk.examples.BigSiftCLI "$@"
    ;;

  benchmark)
    shift
    case "${1:-capture}" in
      capture)  submit org.bigasterisk.examples.CaptureOverheadBenchmark ;;
      ablation) submit org.bigasterisk.examples.AblationBenchmark ;;
      fuzz)     submit org.bigasterisk.examples.FuzzAbstractionBenchmark ;;
      *) echo "unknown benchmark '$1' (capture|ablation|fuzz)" >&2; exit 2 ;;
    esac
    ;;

  submit)
    # escape hatch: run any main class in the image against the cluster
    shift
    [ $# -ge 1 ] || { echo "usage: submit <class> [args...]" >&2; exit 2; }
    submit "$@"
    ;;

  airline)
    # The airline pipeline and every tool on it, against the cluster.
    mkdir -p "$EVENT_LOG_DIR"
    exec "$SPARK_HOME/bin/spark-submit" \
      --master "$MASTER_URL" \
      --jars "$(jars)" \
      --conf spark.eventLog.enabled=true \
      --conf "spark.eventLog.dir=$EVENT_LOG_DIR" \
      --conf "spark.executor.cores=${EXECUTOR_CORES:-2}" \
      --conf "spark.executor.memory=${EXECUTOR_MEMORY:-2g}" \
      --conf "spark.cores.max=${CORES_MAX:-6}" \
      /opt/bigasterisk/python/demos/airline_analysis.py
    ;;

  notebook)
    # JupyterLab whose driver is a client of this cluster. The executors live in the
    # worker containers, so the driver has to be reachable by name from them — hence
    # SPARK_DRIVER_HOST, which Compose sets to this service's name.
    mkdir -p "$EVENT_LOG_DIR"
    export BIGASTERISK_MASTER="$MASTER_URL"
    export SPARK_DRIVER_HOST="${SPARK_DRIVER_HOST:-notebook}"
    export BIGASTERISK_JARS="$(jars)"
    cd /opt/bigasterisk/notebooks
    exec jupyter lab --ip 0.0.0.0 --port 8888 --no-browser --allow-root \
      --IdentityProvider.token='' --ServerApp.password=''
    ;;

  pydemo)
    # The PySpark front end against the cluster, including reading inside a Python UDF.
    mkdir -p "$EVENT_LOG_DIR"
    exec "$SPARK_HOME/bin/spark-submit" \
      --master "$MASTER_URL" \
      --jars "$(jars)" \
      --conf spark.eventLog.enabled=true \
      --conf "spark.eventLog.dir=$EVENT_LOG_DIR" \
      --conf "spark.executor.cores=${EXECUTOR_CORES:-1}" \
      --conf "spark.executor.memory=${EXECUTOR_MEMORY:-1g}" \
      --conf "spark.cores.max=${CORES_MAX:-4}" \
      /opt/bigasterisk/python/demos/cluster_demo.py
    ;;

  pyspark)
    # An interactive PySpark shell attached to the cluster, with the platform loaded.
    exec "$SPARK_HOME/bin/pyspark" --master "$MASTER_URL" --jars "$(jars)"
    ;;

  shell)
    exec /bin/sh
    ;;

  help|*)
    cat <<'USAGE'
BigAsterisk cluster image. Roles:

  master              standalone master (port 7077, UI 8080)
  worker              standalone worker (UI 8081)
  history             history server (UI 18080)

  analyze [args]      run any tool against YOUR query and data (--help for the flags)
  tour [tool...]      a smoke test: the bundled example, every tool. Name tools to run
                      only those (desql titian flowdebug bigsift optdebug bigdebug
                      perfdebug vega bigfuzz depfuzz naturalfuzz bigtest naturalsym)
  bigsift [args]      run the BigSift CLI
  benchmark <name>    capture | ablation | fuzz
  submit <class>      submit any main class in the image
  notebook            JupyterLab, with its driver attached to this cluster
  airline             a realistic airline pipeline, with every tool on it
  pydemo              the PySpark front end against the cluster, end to end
  pyspark             interactive PySpark shell attached to the cluster
  shell               a shell in the image
USAGE
    ;;
esac
