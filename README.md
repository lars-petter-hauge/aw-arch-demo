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

- `k8s/worker-a-scaler.yaml` — Scales worker-a from 1 to 10 replicas when `queue:worker_a` has pending jobs
- `k8s/worker-b-scaler.yaml` — Scales worker-b from 1 to 10 replicas when `queue:worker_b` has pending jobs

Both scalers poll every 5 seconds. Worker A cools down after 30s of idle, Worker B after 60s (since its jobs take longer).

The scalers are applied automatically with `kubectl apply -f k8s/`.

## Deployment (Radix)

Deployed to Radix with each component as a separate container, independently scalable. See `radixconfig.yaml`.
