#!/bin/sh
set -e

SPARK_HOME="$(python3 -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
export SPARK_HOME
# Every jar the platform ships, plus fastutil, as one comma-separated --jars value.
BIGASTERISK_JARS="$(find /opt/bigasterisk/jars -name '*.jar' | paste -sd, -)"
export BIGASTERISK_JARS

case "${1:-lab}" in
  validate)
    # execute every notebook headlessly; any failed assertion fails the run
    cd /opt/bigasterisk/notebooks
    status=0
    for nb in *.ipynb; do
      echo "=== executing $nb"
      if jupyter nbconvert --to notebook --execute --stdout \
           --ExecutePreprocessor.timeout=600 "$nb" > /dev/null; then
        echo "=== $nb OK"
      else
        echo "=== $nb FAILED"
        status=1
      fi
    done
    [ "$status" -eq 0 ] && echo "ALL NOTEBOOKS VALIDATED"
    exit "$status"
    ;;
  lab)
    cd /opt/bigasterisk/notebooks
    exec jupyter lab --ip 0.0.0.0 --port 8888 --no-browser --allow-root \
      --NotebookApp.token=''
    ;;
  *)
    exec "$@"
    ;;
esac
