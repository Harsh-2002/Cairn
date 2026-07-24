# Container & Kubernetes deployment

> Operator guide. Cairn is a single static binary that stores everything on **one POSIX
> filesystem** (object bytes as files + the SQLite metadata DB). The deployment rules follow from
> that: give it durable, single-writer storage and treat it as a stateful service. See
> [`operations.md`](./operations.md) for the config table and the one-filesystem invariant, and
> [`configuration.md`](./configuration.md) for every `CAIRN_*` knob.

## 1. Container

A `Dockerfile` ships in the repo (static musl binary → minimal image). Two listeners and three
operational endpoints to know:

- **`:7373`** — the S3 data plane (`CAIRN_LISTEN_ADDR`). Also serves `/healthz`, `/readyz`, `/metrics`.
- **`:7374`** — the web console + management API (`CAIRN_WEB_ADDR`). Set `CAIRN_WEB_ADDR=off` for headless.
- Data lives under `CAIRN_DATA_DIR` (with `CAIRN_DB_PATH` inside it) — mount a **persistent volume** there.

Minimal run:

```sh
docker run -d --name cairn \
  -p 7373:7373 -p 7374:7374 \
  -v /srv/cairn:/data \
  -e CAIRN_DATA_DIR=/data -e CAIRN_DB_PATH=/data/cairn.db \
  -e CAIRN_MASTER_KEY="$(openssl rand -hex 32)" \
  -e CAIRN_ROOT_ACCESS_KEY=... -e CAIRN_ROOT_SECRET_KEY=... \
  ghcr.io/you/cairn:TAG serve
```

> Keep the `CAIRN_MASTER_KEY` constant for the life of the data: it seals every secret at rest, and
> losing it means losing the ability to open sealed data. Manage it as a secret, not an inline env in
> a committed manifest.

## 2. Kubernetes

Cairn is **single-node** (no clustering); deploy it as a **StatefulSet with one replica** bound to a
PersistentVolume. Do **not** scale the replica count to share one PVC — the single writer owns the
database. For redundancy or read offload, run a *second* StatefulSet and wire bucket replication
([`replication.md`](./replication.md)); for zero-downtime upgrades, see
[`upgrade-rollback.md`](./upgrade-rollback.md) §2.

```yaml
apiVersion: v1
kind: Secret
metadata: { name: cairn-secrets }
type: Opaque
stringData:
  CAIRN_MASTER_KEY: "<32-byte hex — generate once, never rotate the value casually>"
  CAIRN_ROOT_ACCESS_KEY: "<root access key>"
  CAIRN_ROOT_SECRET_KEY: "<root secret>"
---
apiVersion: v1
kind: ConfigMap
metadata: { name: cairn-config }
data:
  CAIRN_DATA_DIR: "/data"
  CAIRN_DB_PATH: "/data/cairn.db"
  CAIRN_LISTEN_ADDR: "0.0.0.0:7373"
  CAIRN_WEB_ADDR: "0.0.0.0:7374"
  # CAIRN_META_SYNCHRONOUS: "full"   # default; see scaling-limits.md before relaxing
  # CAIRN_META_SHARDS: "1"           # LOCKED at first init — pick up front (scaling-limits.md §3)
---
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: cairn }
spec:
  serviceName: cairn
  replicas: 1                         # single-node: never >1 on one PVC
  selector: { matchLabels: { app: cairn } }
  template:
    metadata: { labels: { app: cairn } }
    spec:
      containers:
        - name: cairn
          image: ghcr.io/you/cairn:TAG
          args: ["serve"]
          ports:
            - { name: s3, containerPort: 7373 }
            - { name: console, containerPort: 7374 }
          envFrom:
            - configMapRef: { name: cairn-config }
            - secretRef: { name: cairn-secrets }
          volumeMounts:
            - { name: data, mountPath: /data }
          readinessProbe:                 # gates traffic; ready only after migrations + reconcile
            httpGet: { path: /readyz, port: s3 }
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:                  # pure liveness; bypasses the concurrency limiter
            httpGet: { path: /healthz, port: s3 }
            periodSeconds: 15
          # Give shutdown time to drain in-flight requests (graceful SIGTERM).
          terminationGracePeriodSeconds: 60
  volumeClaimTemplates:
    - metadata: { name: data }
      spec:
        accessModes: ["ReadWriteOnce"]    # one writer; RWO is correct
        resources: { requests: { storage: 100Gi } }
```

### Probes
- **`/readyz`** is the traffic gate: it returns `ready` only after startup migrations + reconciliation
  complete and both the read pool and the writer are responsive. Use it for `readinessProbe`.
- **`/healthz`** is pure liveness and **bypasses the concurrency limiter**, so a loaded node still
  reports live (it won't be killed for shedding load). Use it for `livenessProbe`.

### Storage
- The PVC **is** your durability domain. Cairn provides no internal RAID/redundancy; put the volume on
  redundant storage (replicated block, or a node with ZFS/RAID). `synchronous=full` (default) makes an
  acknowledged write survive power loss *given the disk honours fsync* — verify your CSI/storage does.
- `ReadWriteOnce` is correct: a single node owns the data. Never share the PVC across replicas.

### TLS
Terminate TLS at Cairn (set `CAIRN_TLS_CERT_PATH`/`CAIRN_TLS_KEY_PATH`; SIGHUP reloads) or at an
ingress/load balancer in front. If terminating upstream, the S3 endpoint clients use must match what
SigV4 signed (host + scheme) — set `CAIRN_PUBLIC_URL` accordingly.

## 3. Backups in K8s
Snapshot per [`backup-restore.md`](./backup-restore.md): a CronJob (or sidecar) runs `cairn backup` to
a separate volume / object store, database-first then blobs. A PVC VolumeSnapshot alone is acceptable
only if it is crash-consistent for the whole filesystem at one instant; the `cairn backup` procedure is
the supported, ordering-correct path.
