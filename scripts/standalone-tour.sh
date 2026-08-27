#!/bin/sh
# Run the platform tour against a real Spark standalone cluster — a separate master
# process, a separate worker process, and executors in their own JVMs — with the
# platform attached the way a user attaches it, via `--jars`.
#
#   scripts/standalone-tour.sh
#
# Why this is not the same as the in-process tour: `local[*]` runs executors inside the
# driver JVM, sharing its classloader. A closure that only the driver's classloader can
# resolve works there and fails on any real cluster. This catches that, and it needs no
# Docker — just the Spark distribution the project already fetches.
set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

JDK="$(ls -d "$ROOT"/tools/jdk-17* 2>/dev/null | head -1)"
if [ -n "$JDK" ]; then
  if [ -d "$JDK/Contents/Home" ]; then JAVA_HOME="$JDK/Contents/Home"; else JAVA_HOME="$JDK"; fi
  export JAVA_HOME
fi

SPARK_HOME="${SPARK_HOME:-$(ls -d "$ROOT"/tools/spark-*-bin-hadoop* 2>/dev/null | head -1)}"
if [ ! -x "$SPARK_HOME/sbin/start-master.sh" ]; then
  echo "No Spark distribution at $SPARK_HOME. Run bin/bootstrap, or set SPARK_HOME." >&2
  exit 1
fi
export SPARK_HOME

WORK="${TMPDIR:-/tmp}/bigasterisk-standalone"
SPARK_LOG_DIR="$WORK/logs"
SPARK_WORKER_DIR="$WORK/work"
SPARK_MASTER_HOST=127.0.0.1
export SPARK_LOG_DIR SPARK_WORKER_DIR SPARK_MASTER_HOST
mkdir -p "$SPARK_LOG_DIR" "$SPARK_WORKER_DIR"

MASTER_URL="spark://127.0.0.1:7077"

cleanup() {
  "$SPARK_HOME/sbin/stop-worker.sh" >/dev/null 2>&1 || true
  "$SPARK_HOME/sbin/stop-master.sh" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

jars() {
  found=$(ls modules/*/target/scala-2.13/bigasterisk-*.jar \
             examples/target/scala-2.13/bigasterisk-examples*.jar 2>/dev/null | paste -sd, -)
  if [ -z "$found" ]; then
    echo "No jars. Run: bin/sbt package" >&2
    exit 1
  fi
  fastutil=$(find "$HOME/Library/Caches/Coursier" "$HOME/.cache/coursier" \
    -name 'fastutil-8.5.15.jar' 2>/dev/null | head -1)
  if [ -n "$fastutil" ]; then echo "$found,$fastutil"; else echo "$found"; fi
}

echo "== starting standalone master"
"$SPARK_HOME/sbin/start-master.sh" >/dev/null

# The worker registers asynchronously; submitting before it does gets an application
# that waits forever for resources, which reads as a hang rather than an error.
echo "== starting worker"
"$SPARK_HOME/sbin/start-worker.sh" "$MASTER_URL" -c 2 -m 2G >/dev/null

registered=0
i=0
while [ "$i" -lt 30 ]; do
  if curl -s http://127.0.0.1:8080 2>/dev/null | grep -q 'Cores in use'; then
    registered=1
    break
  fi
  sleep 2
  i=$((i + 1))
done
if [ "$registered" -ne 1 ]; then
  echo "The worker did not register within 60s. Master log:" >&2
  tail -40 "$SPARK_LOG_DIR"/*master*.out >&2 || true
  exit 1
fi

MAIN_JAR=$(ls examples/target/scala-2.13/bigasterisk-examples*.jar | head -1)
echo "== submitting the tour to $MASTER_URL"
"$SPARK_HOME/bin/spark-submit" \
  --master "$MASTER_URL" \
  --class org.bigasterisk.examples.PlatformTour \
  --jars "$(jars)" \
  --conf spark.executor.cores=2 \
  --conf spark.cores.max=2 \
  --conf spark.executor.memory=1500m \
  "$MAIN_JAR"

echo "== cluster-mode tour OK"
