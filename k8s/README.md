# BigAsterisk on Kubernetes

Spark's Kubernetes scheduler needs three things that differ from standalone: a service
account the driver can create executor pods with, an image both driver and executors run,
and a way to get the platform jars onto both. The manifests here provide the first, the
`docker/Dockerfile.cluster` image provides the second, and the third is a `--jars`
argument — the same one a standalone submit uses.

```bash
kubectl apply -f k8s/rbac.yaml
scripts/k8s-submit.sh tour
```

| File | What it is |
|---|---|
| `rbac.yaml` | namespace, service account, and the role a Spark driver needs to manage its executors |
| `../scripts/k8s-submit.sh` | wraps `spark-submit --master k8s://…` with the settings below |

## Not verified

**These manifests have not been run against a cluster.** Everything else in this
repository is executed before it is documented; this is the exception, and it is called
out rather than left for you to discover. There was no reachable Kubernetes cluster on
the machine they were written on — no `kind`, `minikube` or Docker Desktop Kubernetes,
and the configured GKE contexts were stale.

What *is* verified is the part these share with everything else: the platform attaches
through `--jars` and `spark.sql.extensions`, and that path is exercised on a real
standalone cluster on every CI run (`scripts/standalone-tour.sh`). Kubernetes changes who
starts the executors, not how the tools attach.

If you run them, the likely rough edges are image pull policy on a local cluster (an
image built locally is not in any registry — use `kind load docker-image` or
`minikube image load`), and whether your cluster's default service account already has
the permissions in `rbac.yaml`.

## What the submit sets, and why

```bash
--master k8s://https://<api-server>
--deploy-mode cluster
--conf spark.kubernetes.container.image=bigasterisk-cluster:latest
--conf spark.kubernetes.authenticate.driver.serviceAccountName=bigasterisk
--conf spark.kubernetes.namespace=bigasterisk
--jars local:///opt/bigasterisk/jars/...
```

`local://` rather than a filesystem path: the jars are baked into the image, so every
executor already has them and nothing needs uploading. That is the one real difference
from a standalone submit, where `--jars` ships them from the client.

`--deploy-mode cluster` puts the driver in a pod. The tools that read the driver-side
analyzed plan — every one of them — work there exactly as they do anywhere else; what
changes is that you read the output with `kubectl logs` rather than from your terminal.

## Limitations

- **Client mode is not covered.** Running the driver outside the cluster needs a
  headless service so executors can reach it back, which is a networking question rather
  than a platform one.
- **No YARN.** The same `--jars` and `spark.sql.extensions` attachment should apply, but
  nothing here proves it.
- **The image is single-architecture.** Built for the machine that built it; a cluster of
  a different architecture needs a rebuild.
