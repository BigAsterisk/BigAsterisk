#!/bin/sh
# Executes every notebook headlessly. Each one ends in assertions, so a clean run is a
# real check that the tools still do what the notebooks say they do.
#
#   scripts/validate-notebooks.sh              # all of them
#   scripts/validate-notebooks.sh optdebug     # one, by name
#
# Requires `bin/sbt package` first, and the toolchain from bin/bootstrap.
set -e
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

JDK="$(ls -d "$ROOT"/tools/jdk-17* 2>/dev/null | head -1)"
if [ -n "$JDK" ]; then
  if [ -d "$JDK/Contents/Home" ]; then JAVA_HOME="$JDK/Contents/Home"; else JAVA_HOME="$JDK"; fi
  export JAVA_HOME
fi

SPARK_HOME="${SPARK_HOME:-$ROOT/tools/spark-4.1.2-bin-hadoop3}"
if [ ! -d "$SPARK_HOME" ]; then
  echo "No Spark distribution at $SPARK_HOME. Run bin/bootstrap, or set SPARK_HOME." >&2
  exit 1
fi
export SPARK_HOME

PYTHON="${NOTEBOOK_PYTHON:-$ROOT/tools/python/bin/python3}"
if [ ! -x "$PYTHON" ]; then PYTHON="$(command -v python3)"; fi
export PYSPARK_PYTHON="$PYTHON"

# Use the Spark distribution's own PySpark rather than a separately installed copy, so
# the notebooks run against exactly the Spark the jars were built for.
PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.9-src.zip:$ROOT/python"
export PYTHONPATH
export BIGASTERISK_HOME="$ROOT"

if [ -n "$1" ]; then
  NOTEBOOKS="$ROOT/notebooks/$1.ipynb"
else
  NOTEBOOKS="$(ls "$ROOT"/notebooks/*.ipynb)"
fi

status=0
for nb in $NOTEBOOKS; do
  name="$(basename "$nb")"
  printf '%-26s ' "$name"
  if "$PYTHON" -m nbconvert --to notebook --execute --stdout \
       --ExecutePreprocessor.timeout=600 "$nb" > /dev/null 2> "/tmp/nb-$name.log"; then
    echo "ok"
  else
    echo "FAILED"
    sed -n 's/^\(.*Error.*\)$/    \1/p' "/tmp/nb-$name.log" | tail -3
    status=1
  fi
done

if [ "$status" -eq 0 ]; then
  echo "all notebooks executed cleanly"
else
  echo "some notebooks failed; see /tmp/nb-*.log" >&2
fi
exit "$status"
