#!/bin/sh
# Bring up a real Spark standalone cluster running BigAsterisk, and submit into it.
#
#   scripts/cluster.sh up [workers]     start master + N workers + history server
#   scripts/cluster.sh tour             run every tool on the cluster
#   scripts/cluster.sh tour titian      run only the tools you name
#   scripts/cluster.sh run <cmd> [...]  any client command (bigsift, benchmark, pyspark)
#   scripts/cluster.sh status           what the master sees
#   scripts/cluster.sh logs [service]   follow logs
#   scripts/cluster.sh down             stop everything and remove the event log volume
#
# The first `up` builds the image, which compiles the platform from source: several
# minutes. Afterwards it is cached.
set -e

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE="docker compose -f $ROOT/docker/compose.yaml"

if ! docker info >/dev/null 2>&1; then
  echo "The Docker daemon is not reachable. Start Docker Desktop (or dockerd) first." >&2
  exit 1
fi

case "${1:-up}" in
  up)
    workers="${2:-2}"
    $COMPOSE up -d --build --scale "worker=$workers"
    echo
    echo "master UI:  http://localhost:8080   (expect $workers workers registered)"
    echo "history:    http://localhost:18080"
    echo
    echo "Then: scripts/cluster.sh tour"
    ;;

  tour)
    shift
    # --rm: the client is a job, not a service. Extra arguments name the tools to run.
    $COMPOSE run --rm submit tour "$@"
    ;;

  run)
    shift
    [ $# -ge 1 ] || { echo "usage: scripts/cluster.sh run <command> [args...]" >&2; exit 2; }
    $COMPOSE run --rm submit "$@"
    ;;

  status)
    $COMPOSE ps
    echo
    # the master's own view is the one that matters: a container can be up while its
    # worker failed to register
    curl -s http://localhost:8080 2>/dev/null \
      | sed -n 's/.*\(Alive Workers:[^<]*\).*/\1/p;s/.*\(Cores in use:[^<]*\).*/\1/p' \
      | head -2 || echo "master UI not reachable yet"
    ;;

  logs)
    shift
    $COMPOSE logs -f "$@"
    ;;

  down)
    $COMPOSE --profile client down -v
    ;;

  *)
    sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
