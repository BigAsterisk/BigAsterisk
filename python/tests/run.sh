#!/bin/sh
# Runs the PySpark end-to-end tests against the local build.
#
#   bin/sbt package && python/tests/run.sh
#
# Uses the project-local toolchain in tools/ (see bin/bootstrap). Override any of
# SPARK_HOME, JAVA_HOME or PYSPARK_PYTHON to point elsewhere.
set -e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# Prefer the project-local JDK 17: an inherited JAVA_HOME often points at an older JDK.
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

# PySpark 4.1 needs Python 3.10+: its sql/types.py evaluates PEP 604 unions
# (`int | None`) at class-body time, which 3.9 cannot parse.
if [ -z "$PYSPARK_PYTHON" ]; then
  if [ -x "$ROOT/tools/python/bin/python3" ]; then
    PYSPARK_PYTHON="$ROOT/tools/python/bin/python3"
  else
    PYSPARK_PYTHON="$(command -v python3 || true)"
  fi
fi
if [ -z "$PYSPARK_PYTHON" ]; then
  echo "No python3 found. Set PYSPARK_PYTHON to a Python 3.9+ interpreter." >&2
  exit 1
fi
if ! "$PYSPARK_PYTHON" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)'; then
  echo "PySpark 4.1 needs Python 3.10+, but $PYSPARK_PYTHON is $("$PYSPARK_PYTHON" -V 2>&1)." >&2
  echo "Set PYSPARK_PYTHON to a newer interpreter." >&2
  exit 1
fi
export PYSPARK_PYTHON
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"

jar() {
  found="$(ls "$ROOT/modules/$1/target/scala-2.13"/bigasterisk-"$1"_2.13-*.jar 2>/dev/null | head -1)"
  if [ -z "$found" ]; then
    echo "Missing jar for module '$1'. Run: bin/sbt package" >&2
    exit 1
  fi
  echo "$found"
}

JARS="$(jar api),$(jar spark4),$(jar bigsift),$(jar optdebug)"
FASTUTIL_JAR="$(find "$HOME/Library/Caches/Coursier" "$HOME/.cache/coursier" \
  -name 'fastutil-8.5.15.jar' 2>/dev/null | head -1)"
[ -n "$FASTUTIL_JAR" ] && JARS="$JARS,$FASTUTIL_JAR"

# Ship the whole bigasterisk package: --py-files takes a zip for a package directory.
PYZIP="$ROOT/python/target/bigasterisk.zip"
mkdir -p "$(dirname "$PYZIP")"
rm -f "$PYZIP"
( cd "$ROOT/python" && zip -qr "$PYZIP" bigasterisk -x '*.pyc' -x '*__pycache__*' )

submit() {
  echo "=== $(basename "$1")"
  BIGASTERISK_HOME="$ROOT" "$SPARK_HOME/bin/spark-submit" \
    --master 'local[2]' \
    --jars "$JARS" \
    --py-files "$PYZIP" \
    "$1"
}

submit "$ROOT/python/tests/test_lineage_pyspark.py"
submit "$ROOT/python/tests/test_bigsift_pyspark.py"
submit "$ROOT/python/tests/test_desql_pyspark.py"
submit "$ROOT/python/tests/test_watchpoint_pyspark.py"
submit "$ROOT/python/tests/test_vega_pyspark.py"
submit "$ROOT/python/tests/test_optdebug_pyspark.py"
