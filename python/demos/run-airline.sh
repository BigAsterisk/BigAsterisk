#!/bin/sh
# Run the airline analysis against a local Spark, with the platform attached.
#
#   bin/sbt package && python/demos/run-airline.sh
#
# FLIGHTS=1000000 python/demos/run-airline.sh   for a larger run
set -e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

JDK="$(ls -d "$ROOT"/tools/jdk-17* 2>/dev/null | head -1)"
if [ -n "$JDK" ]; then
  if [ -d "$JDK/Contents/Home" ]; then JAVA_HOME="$JDK/Contents/Home"; else JAVA_HOME="$JDK"; fi
  export JAVA_HOME
fi
SPARK_HOME="${SPARK_HOME:-$(ls -d "$ROOT"/tools/spark-*-bin-hadoop* 2>/dev/null | head -1)}"
export SPARK_HOME
PYSPARK_PYTHON="${PYSPARK_PYTHON:-$ROOT/tools/python/bin/python3}"
[ -x "$PYSPARK_PYTHON" ] || PYSPARK_PYTHON="$(command -v python3)"
export PYSPARK_PYTHON PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"

JARS="$(ls "$ROOT"/modules/*/target/scala-2.13/bigasterisk-*.jar 2>/dev/null | paste -sd, -)"
FASTUTIL="$(find "$HOME/Library/Caches/Coursier" "$HOME/.cache/coursier" \
  -name 'fastutil-8.5.15.jar' 2>/dev/null | head -1)"
[ -n "$FASTUTIL" ] && JARS="$JARS,$FASTUTIL"
[ -n "$JARS" ] || { echo "No jars. Run: bin/sbt package" >&2; exit 1; }

exec "$SPARK_HOME/bin/spark-submit" \
  --master "${AIRLINE_MASTER:-local[4]}" \
  --driver-memory "${DRIVER_MEMORY:-4g}" \
  --jars "$JARS" \
  --py-files "$ROOT/python" \
  "$ROOT/python/demos/airline_analysis.py"
