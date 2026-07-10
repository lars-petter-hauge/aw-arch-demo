# AW Architecture Demo

Proof-of-concept demonstrating how to decouple a web API from compute-heavy model workers using an async job queue.

## Problem

In the current setup, the backend both serves the API and runs compute models directly. When a heavy model runs, the API becomes unresponsive. Additionally, different models have different dependencies and may be written in different languages — cramming them into one container is impractical.

## Architecture

```
┌──────────┐       ┌───────┐       ┌───────┐
│  Client  │──────▶│  API  │──────▶│ Redis │
└──────────┘       └───────┘       └───┬───┘
                                       │
                             ┌─────────┼─────────┐
                             ▼                   ▼
                       ┌──────────┐        ┌──────────┐
                       │ Worker A │        │ Worker B │
                       │  (~1s)   │        │ (~20-30s)│
                       └──────────┘        └──────────┘
```

### Components

| Component | Description |
|-----------|-------------|
| **API** | FastAPI server. Exposes endpoints to trigger runs and check status. Does not run any models itself. |
| **Redis** | Message broker and result store. Jobs are enqueued via LPUSH/BRPOP. Results stored as `result:{job_id}`. |
| **Worker A** | Lightweight model. Consumes jobs from `queue:worker_a`. Burns ~1 second of CPU (single core, 100%). |
| **Worker B** | Heavy model. Consumes jobs from `queue:worker_b`. Burns ~20-30 seconds of CPU (single core, 100%). |

### Workflow

The API exposes an endpoint that triggers a **pipeline of 10 sequential runs**:

For each of the 10 iterations:
1. **Model A** runs first (~1s CPU)
2. Once Model A completes, **Model B** runs (~20-30s CPU)

So the full sequence is: A → B → A → B → ... (10 pairs).

The API remains responsive throughout — all compute is offloaded to workers via Redis.

### Endpoints

```
POST /pipeline
```

Returns a pipeline ID. The client can poll for status:

```
GET /pipeline/{pipeline_id}
```

Returns the current state: which iteration we're on, whether each step is pending/running/completed, and final results.

## Running locally (Docker Compose)

```bash
docker-compose up --build
```

The API is available at `http://localhost:8000`.

## Running locally (Kubernetes)

Requires [minikube](https://minikube.sigs.k8s.io/) or [kind](https://kind.sigs.k8s.io/).

```bash
# Build images
docker build -t aw-arch-demo-api ./api
docker build -t aw-arch-demo-worker-a ./worker_a
docker build -t aw-arch-demo-worker-b ./worker_b

# Load images into minikube
minikube image load aw-arch-demo-api:latest
minikube image load aw-arch-demo-worker-a:latest
minikube image load aw-arch-demo-worker-b:latest

# Deploy
kubectl apply -f k8s/

# Get the API URL
minikube service api --url
```

The API is exposed on NodePort 30080.

### Auto-scaling with KEDA

[KEDA](https://keda.sh/) scales workers based on Redis queue length — when jobs pile up, more worker pods are created automatically.

**Install KEDA:**

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace
```

**How it works:**

- Workers scale from **0 to 10 replicas** based on queue depth (scale-to-zero when idle)
- KEDA polls Redis every 5 seconds
- After the queue drains, workers stay alive for **1 hour** (`cooldownPeriod: 3600`) before scaling back to 0 — this avoids repeated cold starts during bursty workloads

**Tuning `listLength` (scaling sensitivity):**

The `listLength` parameter controls how aggressively KEDA scales. It represents the number of queued items per replica — KEDA calculates desired replicas as `queueLength / listLength`.

**Rule of thumb:** set `listLength` to approximately how many jobs one worker can process within one polling interval (5 seconds).

| Worker | Job duration | Jobs/5s/replica | listLength | Effect |
|--------|-------------|-----------------|------------|--------|
| Worker A | ~10ms | ~500 | `500` | Only scales up when backlog exceeds what one replica handles in a poll cycle |
| Worker B | ~25s | ~0.2 | `2` | Scales up quickly since each replica is slow |

**Examples:**
- 100 jobs in `queue:worker_a` → 100/500 = 0.2 → stays at 1 replica (one worker handles it in <1s)
- 1000 jobs in `queue:worker_a` → 1000/500 = 2 replicas
- 10 jobs in `queue:worker_b` → 10/2 = 5 replicas (each takes ~25s, so 5 replicas finish in ~50s)

The scalers are applied automatically with `kubectl apply -f k8s/`.

## Deployment (Radix)

Deployed to Radix with each component as a separate container, independently scalable. The same KEDA scaling logic is configured via `horizontalScaling` in `radixconfig.yaml` — Radix runs KEDA natively.
