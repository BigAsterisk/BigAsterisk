#!/bin/sh
# Submit a BigAsterisk demo to a Kubernetes cluster.
#
#   kubectl apply -f k8s/rbac.yaml
#   scripts/k8s-submit.sh tour
#   scripts/k8s-submit.sh submit org.example.MyJob
#
# NOT VERIFIED against a cluster — see k8s/README.md. The image must be reachable by the
# cluster: for a local one, load it first (`kind load docker-image bigasterisk-cluster`).
set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${BIGASTERISK_IMAGE:-bigasterisk-cluster:latest}"
NAMESPACE="${BIGASTERISK_NAMESPACE:-bigasterisk}"

SPARK_HOME="${SPARK_HOME:-$ROOT/tools/spark-4.1.2-bin-hadoop3}"
if [ ! -x "$SPARK_HOME/bin/spark-submit" ]; then
  echo "No Spark distribution at $SPARK_HOME. Run bin/bootstrap, or set SPARK_HOME." >&2
  exit 1
fi

MASTER="${K8S_MASTER:-$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null)}"
if [ -z "$MASTER" ]; then
  echo "No Kubernetes API server found. Is kubectl pointed at a cluster?" >&2
  exit 1
fi

case "${1:-tour}" in
  tour)   CLASS=org.bigasterisk.examples.PlatformTour ;;
  submit) shift; CLASS="$1"; shift ;;
  *)      echo "usage: $0 [tour|submit <class>]" >&2; exit 2 ;;
esac

# `local://` paths: the jars are baked into the image, so nothing is uploaded and every
# executor already has them. That is the one real difference from a standalone submit.
JARS="$(printf 'local:///opt/bigasterisk/jars/%s,' \
  bigasterisk-api_2.13-0.1.0-SNAPSHOT.jar \
  bigasterisk-spark4_2.13-0.1.0-SNAPSHOT.jar \
  bigasterisk-bigsift_2.13-0.1.0-SNAPSHOT.jar \
  bigasterisk-optdebug_2.13-0.1.0-SNAPSHOT.jar \
  fastutil.jar | sed 's/,$//')"

exec "$SPARK_HOME/bin/spark-submit" \
  --master "k8s://$MASTER" \
  --deploy-mode cluster \
  --name bigasterisk \
  --class "$CLASS" \
  --jars "$JARS" \
  --conf "spark.kubernetes.container.image=$IMAGE" \
  --conf "spark.kubernetes.namespace=$NAMESPACE" \
  --conf "spark.kubernetes.authenticate.driver.serviceAccountName=bigasterisk" \
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
  --conf spark.executor.instances="${EXECUTORS:-2}" \
  --conf spark.executor.memory="${EXECUTOR_MEMORY:-1g}" \
  "local:///opt/bigasterisk/jars/bigasterisk-examples_2.13-0.1.0-SNAPSHOT.jar" "$@"
